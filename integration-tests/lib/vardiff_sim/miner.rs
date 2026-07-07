//! A single simulated miner: real SV2 protocol, fake work.
//!
//! The task connects to the pool, opens a standard mining channel, then
//! submits dummy shares at exponentially distributed intervals derived from
//! its virtual hashrate and the current channel target. It reacts to
//! `SetTarget` (vardiff adjustments) by resampling its share timer.

use std::{convert::TryInto, net::SocketAddr};

use async_channel::{Receiver, Sender};
use rand::Rng;
use stratum_apps::{
    key_utils::Secp256k1PublicKey,
    network_helpers::noise_connection::Connection,
    stratum_core::{
        codec_sv2::{HandshakeRole, StandardEitherFrame, StandardSv2Frame},
        common_messages_sv2::{Protocol, SetupConnection},
        mining_sv2::{NewMiningJob, OpenStandardMiningChannel, SubmitSharesStandard},
        noise_sv2::Initiator,
        parsers_sv2::{CommonMessages, Mining, MiningDeviceMessages},
    },
};
use tokio::{net::TcpStream, time::Instant};
use tracing::{debug, info, warn};

use super::{sample_share_interval, MinerCommand, MinerConfig, MinerEvent};

pub type Message = MiningDeviceMessages<'static>;
pub type StdFrame = StandardSv2Frame<Message>;
pub type EitherFrame = StandardEitherFrame<Message>;

/// Runs one simulated miner until it is told to disconnect or the connection
/// drops. Events are reported tagged with the miner name.
pub async fn run_miner(
    config: MinerConfig,
    pool_address: SocketAddr,
    authority_pubkey: Option<Secp256k1PublicKey>,
    commands: Receiver<MinerCommand>,
    events: Sender<(String, MinerEvent)>,
) {
    let name = config.name.clone();
    let reason = match run_miner_inner(&config, pool_address, authority_pubkey, commands, &events)
        .await
    {
        Ok(reason) => reason,
        Err(e) => e,
    };
    let _ = events
        .send((name, MinerEvent::Disconnected { reason }))
        .await;
}

async fn run_miner_inner(
    config: &MinerConfig,
    pool_address: SocketAddr,
    authority_pubkey: Option<Secp256k1PublicKey>,
    commands: Receiver<MinerCommand>,
    events: &Sender<(String, MinerEvent)>,
) -> Result<String, String> {
    let name = &config.name;
    let emit = |ev: MinerEvent| {
        let events = events.clone();
        let name = name.clone();
        async move {
            let _ = events.send((name, ev)).await;
        }
    };

    // The pool opens its listener only once it has a template, so tolerate a
    // slow start (and pool restarts) by retrying the whole connection
    // sequence for a while.
    let (receiver, sender) = {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match connect_and_setup(pool_address, authority_pubkey).await {
                Ok(channel) => break channel,
                Err(e) if tokio::time::Instant::now() < deadline => {
                    debug!("{name}: connect failed ({e}), retrying");
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
                Err(e) => return Err(e),
            }
        }
    };
    emit(MinerEvent::Connected).await;
    debug!("{name}: SetupConnection succeeded");

    let open_channel = OpenStandardMiningChannel {
        request_id: 1,
        user_identity: name
            .clone()
            .try_into()
            .map_err(|_| "invalid miner name for user_identity".to_string())?,
        nominal_hash_rate: config.nominal_hashrate(),
        max_target: vec![0xFF_u8; 32]
            .try_into()
            .expect("static max target is valid"),
    };
    send_mining(&sender, Mining::OpenStandardMiningChannel(open_channel)).await?;

    let mut state = MinerState {
        hashrate: config.hashrate,
        channel_id: None,
        target_le: None,
        active_job: None,
        future_jobs: Vec::new(),
        ntime: 0,
        sequence: 0,
    };
    let mut next_share_at: Option<Instant> = None;

    loop {
        let share_timer = async {
            match next_share_at {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            frame = receiver.recv() => {
                let frame = frame.map_err(|e| format!("connection closed: {e:?}"))?;
                let mut frame: StdFrame = frame
                    .try_into()
                    .map_err(|e| format!("bad frame: {e:?}"))?;
                let message_type = frame
                    .get_header()
                    .ok_or_else(|| "frame without header".to_string())?
                    .msg_type();
                let payload = frame.payload();
                let message: Mining<'_> = match (message_type, payload).try_into() {
                    Ok(m) => m,
                    Err(e) => {
                        debug!("{name}: ignoring non-mining message: {e:?}");
                        continue;
                    }
                };
                if let Some(ev) = state.handle_mining_message(name, message)? {
                    emit(ev).await;
                }
                next_share_at = state.resample_share_timer();
            }
            cmd = commands.recv() => {
                match cmd {
                    Ok(MinerCommand::SetHashrate(h)) => {
                        info!("{name}: hashrate {} -> {}", state.hashrate, h);
                        state.hashrate = h;
                        emit(MinerEvent::HashrateChanged { hashrate: h }).await;
                        next_share_at = state.resample_share_timer();
                    }
                    Ok(MinerCommand::Resample) => {
                        next_share_at = state.resample_share_timer();
                    }
                    Ok(MinerCommand::Disconnect) | Err(_) => {
                        return Ok("disconnect requested".to_string());
                    }
                }
            }
            _ = share_timer => {
                if let Some(share) = state.build_share() {
                    let sequence = share.sequence_number;
                    send_mining(&sender, Mining::SubmitSharesStandard(share)).await?;
                    emit(MinerEvent::ShareSubmitted { sequence }).await;
                }
                next_share_at = state.resample_share_timer();
            }
        }
    }
}

struct MinerState {
    hashrate: f64,
    channel_id: Option<u32>,
    target_le: Option<[u8; 32]>,
    /// (job_id, version) of the currently active job.
    active_job: Option<(u32, u32)>,
    future_jobs: Vec<NewMiningJob<'static>>,
    ntime: u32,
    sequence: u32,
}

impl MinerState {
    fn ready(&self) -> bool {
        self.channel_id.is_some() && self.target_le.is_some() && self.active_job.is_some()
    }

    fn resample_share_timer(&self) -> Option<Instant> {
        if !self.ready() {
            return None;
        }
        let target = self.target_le.as_ref()?;
        sample_share_interval(self.hashrate, target).map(|dt| Instant::now() + dt)
    }

    fn build_share(&mut self) -> Option<SubmitSharesStandard> {
        let channel_id = self.channel_id?;
        let (job_id, version) = self.active_job?;
        self.sequence += 1;
        Some(SubmitSharesStandard {
            channel_id,
            sequence_number: self.sequence,
            job_id,
            nonce: rand::thread_rng().gen(),
            ntime: self.ntime,
            version,
        })
    }

    /// Applies a mining message to the miner state; returns an event to
    /// report, if any. Errors end the miner task.
    fn handle_mining_message(
        &mut self,
        name: &str,
        message: Mining<'_>,
    ) -> Result<Option<MinerEvent>, String> {
        match message {
            Mining::OpenStandardMiningChannelSuccess(m) => {
                let target_le: [u8; 32] = m
                    .target
                    .to_owned_bytes()
                    .try_into()
                    .map_err(|_| "target is not 32 bytes".to_string())?;
                self.channel_id = Some(m.channel_id);
                self.target_le = Some(target_le);
                info!("{name}: channel {} opened", m.channel_id);
                Ok(Some(MinerEvent::ChannelOpened {
                    channel_id: m.channel_id,
                    target_le,
                }))
            }
            Mining::OpenMiningChannelError(m) => Err(format!(
                "OpenMiningChannelError: {}",
                String::from_utf8_lossy(m.error_code.as_ref())
            )),
            Mining::SetTarget(m) => {
                let target_le: [u8; 32] = m
                    .maximum_target
                    .to_owned_bytes()
                    .try_into()
                    .map_err(|_| "target is not 32 bytes".to_string())?;
                self.target_le = Some(target_le);
                Ok(Some(MinerEvent::TargetUpdated { target_le }))
            }
            Mining::NewMiningJob(m) => {
                if m.is_future() {
                    self.future_jobs.push(m.into_static());
                    Ok(None)
                } else {
                    if let Some(min_ntime) = m.min_ntime.clone().into_inner() {
                        self.ntime = min_ntime;
                    }
                    self.active_job = Some((m.job_id, m.version));
                    Ok(Some(MinerEvent::NewJob { job_id: m.job_id }))
                }
            }
            Mining::SetNewPrevHash(m) => {
                self.ntime = m.min_ntime;
                let activated = self
                    .future_jobs
                    .iter()
                    .find(|j| j.job_id == m.job_id)
                    .map(|j| (j.job_id, j.version));
                if let Some(job) = activated {
                    self.active_job = Some(job);
                    self.future_jobs.retain(|j| j.job_id != m.job_id);
                    Ok(Some(MinerEvent::NewJob { job_id: job.0 }))
                } else {
                    // Prev hash for a job we don't know; stale jobs are dropped.
                    self.future_jobs.clear();
                    Ok(None)
                }
            }
            Mining::SubmitSharesSuccess(m) => Ok(Some(MinerEvent::SharesAccepted {
                count: m.new_submits_accepted_count,
            })),
            Mining::SubmitSharesError(m) => Ok(Some(MinerEvent::ShareRejected {
                code: String::from_utf8_lossy(m.error_code.as_ref()).to_string(),
            })),
            Mining::CloseChannel(m) => Err(format!(
                "channel closed by pool: {}",
                String::from_utf8_lossy(m.reason_code.as_ref())
            )),
            other => {
                warn!("{name}: unhandled mining message: {other:?}");
                Ok(None)
            }
        }
    }
}

/// TCP connect, noise handshake, and SV2 SetupConnection as one retryable
/// unit.
async fn connect_and_setup(
    pool_address: SocketAddr,
    authority_pubkey: Option<Secp256k1PublicKey>,
) -> Result<(Receiver<EitherFrame>, Sender<EitherFrame>), String> {
    let socket = TcpStream::connect(pool_address)
        .await
        .map_err(|e| format!("tcp connect failed: {e}"))?;
    let initiator = Initiator::new(authority_pubkey.map(|k| k.0));
    let (receiver, sender) = Connection::new(socket, HandshakeRole::Initiator(initiator))
        .await
        .map_err(|e| format!("noise handshake failed: {e:?}"))?;
    setup_connection(&receiver, &sender, pool_address).await?;
    Ok((receiver, sender))
}

async fn setup_connection(
    receiver: &Receiver<EitherFrame>,
    sender: &Sender<EitherFrame>,
    pool_address: SocketAddr,
) -> Result<(), String> {
    let setup = SetupConnection {
        protocol: Protocol::MiningProtocol,
        min_version: 2,
        max_version: 2,
        // REQUIRES_STANDARD_JOBS: we open standard channels only.
        flags: 0b0001,
        endpoint_host: pool_address
            .ip()
            .to_string()
            .into_bytes()
            .try_into()
            .expect("ip string is valid"),
        endpoint_port: pool_address.port(),
        vendor: String::new().try_into().expect("empty string is valid"),
        hardware_version: String::new().try_into().expect("empty string is valid"),
        firmware: String::new().try_into().expect("empty string is valid"),
        device_id: String::new().try_into().expect("empty string is valid"),
    };
    let frame: StdFrame = MiningDeviceMessages::Common(setup.into())
        .try_into()
        .map_err(|e| format!("failed to frame SetupConnection: {e:?}"))?;
    sender
        .send(frame.into())
        .await
        .map_err(|e| format!("failed to send SetupConnection: {e}"))?;

    let mut response: StdFrame = receiver
        .recv()
        .await
        .map_err(|e| format!("connection closed during setup: {e:?}"))?
        .try_into()
        .map_err(|e| format!("bad frame during setup: {e:?}"))?;
    let message_type = response
        .get_header()
        .ok_or_else(|| "frame without header".to_string())?
        .msg_type();
    let payload = response.payload();
    let message: CommonMessages<'_> = (message_type, payload)
        .try_into()
        .map_err(|e| format!("unexpected setup response: {e:?}"))?;
    match message {
        CommonMessages::SetupConnectionSuccess(_) => Ok(()),
        CommonMessages::SetupConnectionError(m) => Err(format!(
            "SetupConnectionError: {}",
            String::from_utf8_lossy(m.error_code.as_ref())
        )),
        other => Err(format!("unexpected setup response: {other:?}")),
    }
}

async fn send_mining(sender: &Sender<EitherFrame>, message: Mining<'static>) -> Result<(), String> {
    let frame: StdFrame = MiningDeviceMessages::Mining(message)
        .try_into()
        .map_err(|e| format!("failed to frame message: {e:?}"))?;
    sender
        .send(frame.into())
        .await
        .map_err(|e| format!("failed to send message: {e}"))
}
