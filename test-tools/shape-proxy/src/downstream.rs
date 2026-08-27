use std::net::SocketAddr;

use stratum_apps::{
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    network_helpers::accept_noise_connection,
    stratum_core::{
        codec_sv2::StandardSv2Frame,
        common_messages_sv2::{Protocol, SetupConnectionSuccess},
        parsers_sv2::{CommonMessages, Mining},
    },
};
use tokio::{net::TcpStream, sync::mpsc};
use tracing::{debug, error, info, warn};

use crate::{
    config::describe_setup_flags,
    upstream::{Message, Reader, Writer},
};

pub type DownstreamId = u64;

pub enum DownstreamEvent {
    Connected {
        id: DownstreamId,
        writer: Writer,
    },
    Message {
        id: DownstreamId,
        msg: Mining<'static>,
    },
    Disconnected {
        id: DownstreamId,
    },
}

impl std::fmt::Debug for DownstreamEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected { id, .. } => f.debug_struct("Connected").field("id", id).finish(),
            Self::Message { id, msg } => f
                .debug_struct("Message")
                .field("id", id)
                .field("msg", msg)
                .finish(),
            Self::Disconnected { id } => f.debug_struct("Disconnected").field("id", id).finish(),
        }
    }
}

/// Per-accept settings, all derived from config and identical for every
/// downstream. Grouped so the accept path takes one parameter instead of four.
#[derive(Clone, Copy)]
pub struct AcceptSettings {
    pub pub_key: Secp256k1PublicKey,
    pub secret_key: Secp256k1SecretKey,
    pub cert_validity: u64,
    /// `SetupConnection.flags` the proxy declared to the pool. Held here only
    /// so the downstream handshake can warn when a miner requires something the
    /// pool was never told about.
    pub declared_upstream_flags: u32,
}

pub async fn accept_downstream(
    stream: TcpStream,
    peer_addr: SocketAddr,
    id: DownstreamId,
    settings: AcceptSettings,
    event_tx: mpsc::UnboundedSender<DownstreamEvent>,
) {
    info!(downstream_id = id, %peer_addr, "Accepting downstream connection");

    let noise_stream = match accept_noise_connection::<Message>(
        stream,
        settings.pub_key,
        settings.secret_key,
        settings.cert_validity,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            error!(downstream_id = id, %peer_addr, "Noise handshake failed: {e}");
            return;
        }
    };

    let (mut reader, mut writer) = noise_stream.into_split();

    if let Err(e) = handle_setup_connection(
        &mut reader,
        &mut writer,
        id,
        settings.declared_upstream_flags,
    )
    .await
    {
        error!(downstream_id = id, "SetupConnection handshake failed: {e}");
        return;
    }

    info!(downstream_id = id, %peer_addr, "Downstream setup complete");

    if event_tx
        .send(DownstreamEvent::Connected { id, writer })
        .is_err()
    {
        return;
    }

    downstream_read_loop(reader, id, event_tx).await;
}

async fn handle_setup_connection(
    reader: &mut Reader,
    writer: &mut Writer,
    id: DownstreamId,
    declared_upstream_flags: u32,
) -> Result<(), String> {
    let response = reader
        .read_frame()
        .await
        .map_err(|e| format!("Read error waiting for SetupConnection: {e:?}"))?;

    let mut frame: StandardSv2Frame<Message> = response
        .try_into()
        .map_err(|_| "Invalid frame from downstream".to_string())?;

    let msg_type = frame.get_header().unwrap().msg_type();
    let payload = frame.payload();

    let msg: CommonMessages = (msg_type, payload)
        .try_into()
        .map_err(|_| format!("Failed to parse message type 0x{msg_type:02x}"))?;

    match msg {
        CommonMessages::SetupConnection(setup) => {
            if setup.protocol != Protocol::MiningProtocol {
                return Err("Downstream requested non-mining protocol".to_string());
            }
            info!(
                downstream_id = id,
                "Downstream SetupConnection: vendor={}, version={}-{}, flags={:#b} ({})",
                setup.vendor.as_utf8_or_hex(),
                setup.min_version,
                setup.max_version,
                setup.flags,
                describe_setup_flags(setup.flags),
            );

            // The proxy re-originates its own SetupConnection upstream, and it
            // does so once at startup — before any downstream exists — so a
            // downstream's flags cannot be forwarded verbatim. Whatever the
            // pool was told is `upstream_setup_flags`. Any requirement bit set
            // here that we did not declare is a requirement the pool never
            // heard about, and the pool is then entitled to serve work that
            // violates it. That silence is what made a correct pool look
            // non-compliant once already, so it is a warning, not a debug line.
            let undeclared = setup.flags & !declared_upstream_flags;
            if undeclared != 0 {
                warn!(
                    downstream_id = id,
                    "Downstream requires {} but the upstream handshake declared {} — \
                     the pool was never told, and may serve work that violates it. \
                     Set `upstream_setup_flags = {:#b}` in the config to relay it.",
                    describe_setup_flags(undeclared),
                    describe_setup_flags(declared_upstream_flags),
                    setup.flags | declared_upstream_flags,
                );
            }
        }
        _ => {
            return Err(format!(
                "Expected SetupConnection, got msg_type=0x{msg_type:02x}"
            ));
        }
    }

    let success = SetupConnectionSuccess {
        used_version: 2,
        flags: 0,
    };
    let frame: StandardSv2Frame<Message> = Message::Common(success.into())
        .try_into()
        .map_err(|e| format!("Frame encode error: {e:?}"))?;
    writer
        .write_frame(frame.into())
        .await
        .map_err(|e| format!("Write error: {e:?}"))?;

    info!(
        downstream_id = id,
        "Sent SetupConnectionSuccess to downstream"
    );
    Ok(())
}

async fn downstream_read_loop(
    mut reader: Reader,
    id: DownstreamId,
    event_tx: mpsc::UnboundedSender<DownstreamEvent>,
) {
    loop {
        let frame = match reader.read_frame().await {
            Ok(f) => f,
            Err(e) => {
                debug!(downstream_id = id, "Downstream read error: {e:?}");
                let _ = event_tx.send(DownstreamEvent::Disconnected { id });
                return;
            }
        };

        let mut sv2_frame: StandardSv2Frame<Message> = match frame.try_into() {
            Ok(f) => f,
            Err(_) => {
                warn!(downstream_id = id, "Invalid frame from downstream");
                let _ = event_tx.send(DownstreamEvent::Disconnected { id });
                return;
            }
        };

        let msg_type = sv2_frame.get_header().unwrap().msg_type();
        let payload = sv2_frame.payload();

        let mining_msg: Result<Mining<'_>, _> = (msg_type, payload).try_into();
        match mining_msg {
            Ok(m) => {
                let m_static = m.into_static();
                debug!(downstream_id = id, "Downstream mining msg: {m_static}");
                if event_tx
                    .send(DownstreamEvent::Message { id, msg: m_static })
                    .is_err()
                {
                    return;
                }
            }
            Err(_) => {
                warn!(
                    downstream_id = id,
                    "Unhandled message type 0x{msg_type:02x} from downstream"
                );
            }
        }
    }
}
