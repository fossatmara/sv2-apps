use std::time::Duration;

use stratum_apps::{
    key_utils::Secp256k1PublicKey,
    network_helpers::{
        connect_with_noise,
        noise_stream::{NoiseTcpReadHalf, NoiseTcpWriteHalf},
        TCP_CONNECT_TIMEOUT,
    },
    stratum_core::{
        codec_sv2::StandardSv2Frame,
        common_messages_sv2::{Protocol, SetupConnection},
        parsers_sv2::{AnyMessage, CommonMessages},
    },
};
use tokio::{net::TcpStream, sync::mpsc};
use tracing::{error, info, warn};

use crate::config::Config;

pub type Message = AnyMessage<'static>;
pub type Reader = NoiseTcpReadHalf<Message>;
pub type Writer = NoiseTcpWriteHalf<Message>;

pub enum UpstreamEvent {
    Connected {
        writer: Writer,
    },
    Frame {
        frame: stratum_apps::stratum_core::codec_sv2::StandardEitherFrame<Message>,
    },
    Disconnected,
}

pub async fn connect_and_setup(config: &Config) -> Result<(Reader, Writer), String> {
    let (mut reader, mut writer) = connect_upstream(config).await?;
    setup_connection(&mut reader, &mut writer).await?;
    Ok((reader, writer))
}

pub async fn upstream_manager_task(config: Config, tx: mpsc::UnboundedSender<UpstreamEvent>) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match connect_and_setup(&config).await {
            Ok((mut reader, writer)) => {
                backoff = Duration::from_secs(1);
                info!("Upstream connection established");
                if tx.send(UpstreamEvent::Connected { writer }).is_err() {
                    return;
                }
                loop {
                    match reader.read_frame().await {
                        Ok(frame) => {
                            if tx.send(UpstreamEvent::Frame { frame }).is_err() {
                                return;
                            }
                        }
                        Err(e) => {
                            warn!("Upstream disconnected: {e:?}");
                            if tx.send(UpstreamEvent::Disconnected).is_err() {
                                return;
                            }
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Upstream connection failed: {e}, retrying in {backoff:?}");
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

pub async fn connect_upstream(config: &Config) -> Result<(Reader, Writer), String> {
    info!("Connecting to upstream pool at {}", config.upstream_address);

    let stream = tokio::time::timeout(
        TCP_CONNECT_TIMEOUT,
        TcpStream::connect(&config.upstream_address),
    )
    .await
    .map_err(|_| "TCP connect timed out".to_string())?
    .map_err(|e| format!("TCP connect failed: {e}"))?;

    info!("TCP connected to {}", config.upstream_address);

    let authority_pub_key: Option<Secp256k1PublicKey> = config
        .upstream_authority_pubkey
        .as_ref()
        .map(|s| s.parse::<Secp256k1PublicKey>())
        .transpose()
        .map_err(|e| format!("Invalid upstream authority pubkey: {e}"))?;

    let noise_stream = connect_with_noise(stream, authority_pub_key)
        .await
        .map_err(|e| format!("Noise handshake failed: {e:?}"))?;

    info!("Noise handshake complete with upstream");
    Ok(noise_stream.into_split())
}

pub async fn setup_connection(reader: &mut Reader, writer: &mut Writer) -> Result<(), String> {
    let setup = SetupConnection {
        protocol: Protocol::MiningProtocol,
        min_version: 2,
        max_version: 2,
        flags: 0,
        endpoint_host: "shape-proxy".to_string().try_into().unwrap(),
        endpoint_port: 0,
        vendor: "shape-proxy".to_string().try_into().unwrap(),
        hardware_version: "".to_string().try_into().unwrap(),
        firmware: "".to_string().try_into().unwrap(),
        device_id: "shape-proxy-0".to_string().try_into().unwrap(),
    };

    let frame: StandardSv2Frame<Message> = Message::Common(setup.into())
        .try_into()
        .map_err(|e| format!("Frame encode error: {e:?}"))?;
    writer
        .write_frame(frame.into())
        .await
        .map_err(|e| format!("Write error: {e:?}"))?;
    info!("Sent SetupConnection to upstream");

    let response = reader
        .read_frame()
        .await
        .map_err(|e| format!("Read error: {e:?}"))?;
    let mut frame: StandardSv2Frame<Message> = response
        .try_into()
        .map_err(|_| "Invalid frame from upstream".to_string())?;
    let msg_type = frame.get_header().unwrap().msg_type();
    let payload = frame.payload();

    let msg: CommonMessages = (msg_type, payload)
        .try_into()
        .map_err(|_| format!("Failed to parse message type {msg_type}"))?;

    match msg {
        CommonMessages::SetupConnectionSuccess(m) => {
            info!(
                "Upstream SetupConnectionSuccess: version={}, flags={}",
                m.used_version, m.flags
            );
            Ok(())
        }
        CommonMessages::SetupConnectionError(m) => {
            let code = std::str::from_utf8(m.error_code.as_ref()).unwrap_or("unknown");
            error!("Upstream SetupConnectionError: {}", code);
            Err(format!("SetupConnectionError: {code}"))
        }
        _ => Err("Unexpected message type during setup".to_string()),
    }
}
