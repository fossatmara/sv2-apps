use std::collections::HashMap;

use stratum_apps::{
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
    stratum_core::{
        codec_sv2::StandardSv2Frame,
        mining_sv2::{OpenExtendedMiningChannel, SubmitSharesSuccess},
        parsers_sv2::Mining,
    },
};
use tokio::{net::TcpListener, select, sync::mpsc};
use tracing::{debug, error, info, warn};

use crate::{
    api::{self, ApiCommand, ChannelStatus, ProfileInfo, ProxyStatus},
    config::Config,
    downstream::{accept_downstream, DownstreamEvent, DownstreamId},
    metrics::{HeadroomStatus, RollingWindow},
    profile::RateProfile,
    share_gate::ShareGate,
    upstream::{self, Message, UpstreamEvent, Writer},
};

struct ChannelMapping {
    downstream_channel_id: u32,
    upstream_channel_id: Option<u32>,
    downstream_id: DownstreamId,
    gate: ShareGate,
    floor_active: bool,
    pool_difficulty: Option<f64>,
    miner_difficulty: f64,
    shares_forwarded: u64,
    shares_gated: u64,
    forward_window: RollingWindow,
    open_request: Option<OpenExtendedMiningChannel<'static>>,
    open_standard_msg: Option<Mining<'static>>,
}

struct PendingChannelOpen {
    downstream_channel_id: u32,
    downstream_id: DownstreamId,
}

struct DownstreamState {
    writer: Writer,
}

pub struct ProxyCore {
    config: Config,
    upstream_writer: Option<Writer>,
    upstream_connected: bool,
    pub_key: Secp256k1PublicKey,
    secret_key: Secp256k1SecretKey,
    next_downstream_id: u64,
    next_channel_id: u32,
    downstreams: HashMap<DownstreamId, DownstreamState>,
    channels: HashMap<u32, ChannelMapping>,
    /// Keyed by the request_id sent upstream. Since we forward the miner's request_id
    /// verbatim and only support one miner, collisions cannot occur in practice.
    pending_opens: HashMap<u32, PendingChannelOpen>,
}

impl ProxyCore {
    pub fn new(config: Config) -> Result<Self, String> {
        let pub_key: Secp256k1PublicKey = config
            .authority_pubkey
            .parse()
            .map_err(|e| format!("Invalid authority_pubkey: {e}"))?;
        let secret_key: Secp256k1SecretKey = config
            .authority_secret
            .parse()
            .map_err(|e| format!("Invalid authority_secret: {e}"))?;

        Ok(Self {
            config,
            upstream_writer: None,
            upstream_connected: false,
            pub_key,
            secret_key,
            next_downstream_id: 1,
            next_channel_id: 1,
            downstreams: HashMap::new(),
            channels: HashMap::new(),
            pending_opens: HashMap::new(),
        })
    }

    pub async fn run(mut self) -> Result<(), String> {
        let listener = TcpListener::bind(self.config.downstream_listen)
            .await
            .map_err(|e| format!("Failed to bind downstream listener: {e}"))?;

        info!(
            "Downstream listener bound to {}",
            self.config.downstream_listen
        );

        let (ds_event_tx, mut ds_event_rx) = mpsc::unbounded_channel::<DownstreamEvent>();
        let (upstream_tx, mut upstream_rx) = mpsc::unbounded_channel::<UpstreamEvent>();

        let upstream_config = self.config.clone();
        tokio::spawn(async move {
            upstream::upstream_manager_task(upstream_config, upstream_tx).await;
        });

        let (api_cmd_tx, mut api_cmd_rx) = mpsc::unbounded_channel::<ApiCommand>();
        let (status_tx, status_rx) = tokio::sync::watch::channel(ProxyStatus::default());

        let api_listen = self.config.api_listen;
        let router = api::create_router(status_rx, api_cmd_tx);
        tokio::spawn(async move {
            let listener = tokio::net::TcpListener::bind(api_listen)
                .await
                .expect("Failed to bind API listener");
            info!("HTTP API listening on {}", api_listen);
            axum::serve(listener, router).await.ok();
        });

        loop {
            let _ = status_tx.send(self.build_status());

            select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            let id = self.next_downstream_id;
                            self.next_downstream_id += 1;
                            let pub_key = self.pub_key;
                            let secret_key = self.secret_key;
                            let cert_validity = self.config.cert_validity_secs;
                            let tx = ds_event_tx.clone();
                            tokio::spawn(async move {
                                accept_downstream(
                                    stream, peer_addr, id,
                                    pub_key, secret_key, cert_validity, tx,
                                ).await;
                            });
                        }
                        Err(e) => {
                            error!("Listener accept error: {e}");
                        }
                    }
                }

                Some(event) = ds_event_rx.recv() => {
                    self.handle_downstream_event(event).await;
                }

                Some(event) = upstream_rx.recv() => {
                    match event {
                        UpstreamEvent::Connected { writer } => {
                            info!("Upstream connected");
                            self.upstream_writer = Some(writer);
                            self.upstream_connected = true;
                            self.reopen_channels().await;
                        }
                        UpstreamEvent::Frame { frame } => {
                            self.handle_upstream_frame(frame).await;
                        }
                        UpstreamEvent::Disconnected => {
                            warn!("Upstream disconnected, will retry in background");
                            self.upstream_writer = None;
                            self.upstream_connected = false;
                            self.pending_opens.clear();
                            for mapping in self.channels.values_mut() {
                                mapping.upstream_channel_id = None;
                            }
                        }
                    }
                }

                Some(cmd) = api_cmd_rx.recv() => {
                    self.handle_api_command(cmd);
                }
            }
        }
    }

    async fn reopen_channels(&mut self) {
        struct Reopen {
            ds_channel_id: u32,
            downstream_id: DownstreamId,
            open_request: Option<OpenExtendedMiningChannel<'static>>,
            open_standard_msg: Option<Mining<'static>>,
        }

        let reopens: Vec<_> = self
            .channels
            .values()
            .filter(|m| self.downstreams.contains_key(&m.downstream_id))
            .map(|m| Reopen {
                ds_channel_id: m.downstream_channel_id,
                downstream_id: m.downstream_id,
                open_request: m.open_request.clone(),
                open_standard_msg: m.open_standard_msg.clone(),
            })
            .collect();

        for Reopen {
            ds_channel_id,
            downstream_id,
            open_request: open_req,
            open_standard_msg: open_std_msg,
        } in reopens
        {
            if let Some(open_req) = open_req {
                info!(
                    downstream_id,
                    ds_channel_id, "Re-opening extended channel after upstream reconnect"
                );
                self.pending_opens.insert(
                    open_req.request_id,
                    PendingChannelOpen {
                        downstream_channel_id: ds_channel_id,
                        downstream_id,
                    },
                );
                let frame: StandardSv2Frame<Message> =
                    match Message::Mining(Mining::OpenExtendedMiningChannel(open_req)).try_into() {
                        Ok(f) => f,
                        Err(e) => {
                            error!("Failed to encode re-open channel: {e:?}");
                            continue;
                        }
                    };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send re-open channel upstream: {e:?}");
                    }
                }
            } else if let Some(std_msg) = open_std_msg {
                info!(
                    downstream_id,
                    ds_channel_id, "Re-opening standard channel after upstream reconnect"
                );
                if let Mining::OpenStandardMiningChannel(ref m) = std_msg {
                    let req_id = m.get_request_id_as_u32();
                    self.pending_opens.insert(
                        req_id,
                        PendingChannelOpen {
                            downstream_channel_id: ds_channel_id,
                            downstream_id,
                        },
                    );
                }
                let frame: StandardSv2Frame<Message> = match Message::Mining(std_msg).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to encode re-open standard channel: {e:?}");
                        continue;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send re-open standard channel upstream: {e:?}");
                    }
                }
            }
        }
    }

    fn handle_api_command(&mut self, cmd: ApiCommand) {
        match cmd {
            ApiCommand::SetProfile {
                channel_id,
                profile,
            } => {
                if let Some(mapping) = self.channels.get_mut(&channel_id) {
                    info!(channel_id, "Setting profile: {:?}", profile);
                    mapping.gate.set_profile(profile);
                } else {
                    warn!(channel_id, "SetProfile for unknown channel");
                }
            }
            ApiCommand::SetAllProfiles { profile } => {
                info!("Broadcasting profile to all channels: {:?}", profile);
                for mapping in self.channels.values_mut() {
                    mapping.gate.set_profile(profile.clone());
                }
            }
        }
    }

    fn build_status(&self) -> ProxyStatus {
        let now = std::time::Instant::now();
        let channels = self
            .channels
            .values()
            .map(|m| {
                let supply_spm = m.gate.current_supply_spm();
                let forwarded_spm = m.forward_window.rate_spm(now);
                let target_spm = m.gate.current_target_spm();
                let headroom = HeadroomStatus::from_ratio(supply_spm, target_spm);
                let profile = m.gate.current_profile();
                let profile_duration = profile.active_duration_secs();
                let profile_elapsed = m.gate.elapsed_secs();
                ChannelStatus {
                    id: m.downstream_channel_id,
                    miner_connected: self.downstreams.contains_key(&m.downstream_id),
                    profile: ProfileInfo::from_profile(profile),
                    profile_elapsed_secs: profile_elapsed,
                    profile_duration_secs: profile_duration,
                    target_spm,
                    forwarded_spm,
                    supply_spm,
                    headroom: headroom.as_str().to_string(),
                    floor_active: m.floor_active,
                    pool_difficulty: m.pool_difficulty,
                    miner_difficulty: m.miner_difficulty,
                    shares_forwarded: m.shares_forwarded,
                    shares_gated: m.shares_gated,
                }
            })
            .collect();

        ProxyStatus {
            upstream_connected: self.upstream_connected,
            channels,
        }
    }

    async fn handle_downstream_event(&mut self, event: DownstreamEvent) {
        match event {
            DownstreamEvent::Connected { id, writer } => {
                info!(downstream_id = id, "Downstream connected and ready");
                self.downstreams.insert(id, DownstreamState { writer });
            }
            DownstreamEvent::Message { id, msg } => {
                self.handle_downstream_message(id, msg).await;
            }
            DownstreamEvent::Disconnected { id } => {
                info!(downstream_id = id, "Downstream disconnected");
                self.downstreams.remove(&id);
                self.channels
                    .retain(|_, mapping| mapping.downstream_id != id);
                self.pending_opens
                    .retain(|_, pending| pending.downstream_id != id);
            }
        }
    }

    async fn handle_downstream_message(&mut self, id: DownstreamId, msg: Mining<'static>) {
        match msg {
            Mining::OpenExtendedMiningChannel(open_req) => {
                self.handle_open_channel(id, open_req).await;
            }
            Mining::OpenStandardMiningChannel(ref m) => {
                let request_id = m.get_request_id_as_u32();
                let ds_channel_id = self.next_channel_id;
                self.next_channel_id += 1;
                info!(
                    downstream_id = id,
                    ds_channel_id, request_id, "Mirroring OpenStandardMiningChannel to upstream"
                );
                if self.pending_opens.contains_key(&request_id) {
                    warn!(
                        request_id,
                        "request_id collision in pending_opens — single-miner assumption violated"
                    );
                }
                self.pending_opens.insert(
                    request_id,
                    PendingChannelOpen {
                        downstream_channel_id: ds_channel_id,
                        downstream_id: id,
                    },
                );
                let open_msg_clone = msg.clone();
                let frame: StandardSv2Frame<Message> = match Message::Mining(msg).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        error!("Failed to encode OpenStandardMiningChannel: {e:?}");
                        return;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to send OpenStandardMiningChannel upstream: {e:?}");
                    }
                } else {
                    warn!(
                        downstream_id = id,
                        "Upstream not connected, cannot open standard channel"
                    );
                }
                self.channels.insert(
                    ds_channel_id,
                    ChannelMapping {
                        downstream_channel_id: ds_channel_id,
                        upstream_channel_id: None,
                        downstream_id: id,
                        gate: ShareGate::new(RateProfile::default()),
                        floor_active: false,
                        pool_difficulty: None,
                        miner_difficulty: 1.0,
                        shares_forwarded: 0,
                        shares_gated: 0,
                        forward_window: RollingWindow::new(),
                        open_request: None,
                        open_standard_msg: Some(open_msg_clone),
                    },
                );
            }
            Mining::SubmitSharesExtended(ref share) => {
                self.handle_share_submission(id, share.channel_id, share.sequence_number, msg)
                    .await;
            }
            Mining::SubmitSharesStandard(ref share) => {
                self.handle_share_submission(id, share.channel_id, share.sequence_number, msg)
                    .await;
            }
            other => {
                debug!(downstream_id = id, "Forwarding message upstream: {other}");
                let frame: StandardSv2Frame<Message> = match Message::Mining(other).try_into() {
                    Ok(f) => f,
                    Err(e) => {
                        warn!(downstream_id = id, "Failed to encode message: {e:?}");
                        return;
                    }
                };
                if let Some(ref mut writer) = self.upstream_writer {
                    if let Err(e) = writer.write_frame(frame.into()).await {
                        error!("Failed to forward message upstream: {e:?}");
                    }
                } else {
                    warn!(
                        downstream_id = id,
                        "Upstream not connected, dropping message"
                    );
                }
            }
        }
    }

    async fn handle_open_channel(
        &mut self,
        downstream_id: DownstreamId,
        open_req: OpenExtendedMiningChannel<'static>,
    ) {
        let ds_channel_id = self.next_channel_id;
        self.next_channel_id += 1;

        let request_id = open_req.request_id;
        info!(
            downstream_id,
            ds_channel_id,
            request_id,
            user_identity = %open_req.user_identity.as_utf8_or_hex(),
            "Mirroring OpenExtendedMiningChannel to upstream"
        );

        if self.pending_opens.contains_key(&request_id) {
            warn!(
                request_id,
                "request_id collision in pending_opens — single-miner assumption violated"
            );
        }
        self.pending_opens.insert(
            request_id,
            PendingChannelOpen {
                downstream_channel_id: ds_channel_id,
                downstream_id,
            },
        );

        self.channels.insert(
            ds_channel_id,
            ChannelMapping {
                downstream_channel_id: ds_channel_id,
                upstream_channel_id: None,
                downstream_id,
                gate: ShareGate::new(RateProfile::default()),
                floor_active: false,
                pool_difficulty: None,
                miner_difficulty: 1.0,
                shares_forwarded: 0,
                shares_gated: 0,
                forward_window: RollingWindow::new(),
                open_request: Some(open_req.clone()),
                open_standard_msg: None,
            },
        );

        let frame: StandardSv2Frame<Message> =
            match Message::Mining(Mining::OpenExtendedMiningChannel(open_req)).try_into() {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to encode OpenExtendedMiningChannel: {e:?}");
                    return;
                }
            };
        if let Some(ref mut writer) = self.upstream_writer {
            if let Err(e) = writer.write_frame(frame.into()).await {
                error!("Failed to send OpenExtendedMiningChannel upstream: {e:?}");
            }
        } else {
            warn!(
                downstream_id,
                "Upstream not connected, channel open queued for reconnect"
            );
        }
    }

    async fn handle_share_submission(
        &mut self,
        id: DownstreamId,
        channel_id: u32,
        seq: u32,
        msg: Mining<'static>,
    ) {
        let ack = SubmitSharesSuccess {
            channel_id,
            last_sequence_number: seq,
            new_submits_accepted_count: 1,
            new_shares_sum: 0,
        };
        self.send_to_downstream(id, Message::Mining(Mining::SubmitSharesSuccess(ack)))
            .await;

        let mapping = self.channels.values_mut().find(|m| {
            m.downstream_id == id
                && (m.downstream_channel_id == channel_id
                    || m.upstream_channel_id == Some(channel_id))
        });

        let Some(mapping) = mapping else {
            debug!(downstream_id = id, channel_id, "Share for unknown channel");
            return;
        };

        let now = std::time::Instant::now();
        let difficulty = mapping.miner_difficulty;
        mapping.gate.record_share_arrived(now, difficulty);

        if !mapping.gate.should_forward() {
            mapping.shares_gated += 1;
            return;
        }

        mapping.shares_forwarded += 1;
        mapping.forward_window.record(now);

        if let Some(ref mut writer) = self.upstream_writer {
            let frame: StandardSv2Frame<Message> = match Message::Mining(msg).try_into() {
                Ok(f) => f,
                Err(e) => {
                    warn!(downstream_id = id, "Failed to encode share: {e:?}");
                    return;
                }
            };
            if let Err(e) = writer.write_frame(frame.into()).await {
                error!("Failed to forward share upstream: {e:?}");
            }
        }
    }

    async fn handle_upstream_frame(
        &mut self,
        frame: stratum_apps::stratum_core::codec_sv2::StandardEitherFrame<Message>,
    ) {
        let mut sv2_frame: StandardSv2Frame<Message> = match frame.try_into() {
            Ok(f) => f,
            Err(_) => {
                warn!("Invalid frame from upstream");
                return;
            }
        };

        let msg_type = sv2_frame.get_header().unwrap().msg_type();
        let payload = sv2_frame.payload();

        let mining_msg: Result<Mining<'_>, _> = (msg_type, payload).try_into();
        match mining_msg {
            Ok(m) => {
                let m_static = m.into_static();
                self.handle_upstream_mining_message(m_static).await;
            }
            Err(_) => {
                debug!("Upstream non-mining message type 0x{msg_type:02x} (ignored)");
            }
        }
    }

    async fn handle_upstream_mining_message(&mut self, msg: Mining<'static>) {
        match msg {
            Mining::OpenExtendedMiningChannelSuccess(ref success) => {
                let request_id = success.request_id;
                let upstream_channel_id = success.channel_id;
                let initial_target_bytes: &[u8] = success.target.inner_as_ref();
                let initial_difficulty = target_to_difficulty(initial_target_bytes);

                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    info!(
                        downstream_id = pending.downstream_id,
                        ds_channel_id = pending.downstream_channel_id,
                        upstream_channel_id,
                        initial_difficulty,
                        "Channel opened successfully"
                    );

                    if let Some(mapping) = self.channels.get_mut(&pending.downstream_channel_id) {
                        mapping.upstream_channel_id = Some(upstream_channel_id);
                        mapping.miner_difficulty = initial_difficulty;
                        mapping.pool_difficulty = Some(initial_difficulty);
                    } else {
                        let gate = ShareGate::new(RateProfile::default());
                        self.channels.insert(
                            pending.downstream_channel_id,
                            ChannelMapping {
                                downstream_channel_id: pending.downstream_channel_id,
                                upstream_channel_id: Some(upstream_channel_id),
                                downstream_id: pending.downstream_id,
                                gate,
                                floor_active: false,
                                pool_difficulty: Some(initial_difficulty),
                                miner_difficulty: initial_difficulty,
                                shares_forwarded: 0,
                                shares_gated: 0,
                                forward_window: RollingWindow::new(),
                                open_request: None,
                                open_standard_msg: None,
                            },
                        );
                    }

                    // Forward the success response to the downstream miner.
                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenExtendedMiningChannelSuccess for unknown request"
                    );
                }
            }
            Mining::OpenStandardMiningChannelSuccess(ref success) => {
                let request_id = success.get_request_id_as_u32();
                let upstream_channel_id = success.channel_id;
                let initial_target_bytes: &[u8] = success.target.inner_as_ref();
                let initial_difficulty = target_to_difficulty(initial_target_bytes);

                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    let effective_ds_channel_id = upstream_channel_id;
                    info!(
                        downstream_id = pending.downstream_id,
                        effective_ds_channel_id,
                        upstream_channel_id,
                        initial_difficulty,
                        "Standard channel opened successfully"
                    );

                    let mut mapping = self
                        .channels
                        .remove(&pending.downstream_channel_id)
                        .unwrap_or_else(|| ChannelMapping {
                            downstream_channel_id: effective_ds_channel_id,
                            upstream_channel_id: Some(upstream_channel_id),
                            downstream_id: pending.downstream_id,
                            gate: ShareGate::new(RateProfile::default()),
                            floor_active: false,
                            pool_difficulty: Some(initial_difficulty),
                            miner_difficulty: initial_difficulty,
                            shares_forwarded: 0,
                            shares_gated: 0,
                            forward_window: RollingWindow::new(),
                            open_request: None,
                            open_standard_msg: None,
                        });
                    mapping.downstream_channel_id = effective_ds_channel_id;
                    mapping.upstream_channel_id = Some(upstream_channel_id);
                    mapping.miner_difficulty = initial_difficulty;
                    mapping.pool_difficulty = Some(initial_difficulty);
                    self.channels.insert(effective_ds_channel_id, mapping);

                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenStandardMiningChannelSuccess for unknown request"
                    );
                }
            }
            Mining::OpenMiningChannelError(ref err) => {
                let request_id = err.request_id;
                if let Some(pending) = self.pending_opens.remove(&request_id) {
                    warn!(
                        downstream_id = pending.downstream_id,
                        request_id,
                        "Channel open rejected by pool: {}",
                        err.error_code.as_utf8_or_hex()
                    );
                    self.channels.remove(&pending.downstream_channel_id);
                    self.send_to_downstream(pending.downstream_id, Message::Mining(msg))
                        .await;
                } else {
                    warn!(
                        request_id,
                        "Received OpenMiningChannelError for unknown request"
                    );
                }
            }
            Mining::NewExtendedMiningJob(ref job) => {
                let upstream_ch = job.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::NewMiningJob(ref job) => {
                let upstream_ch = job.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SetNewPrevHash(ref prev) => {
                let upstream_ch = prev.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SetTarget(ref target) => {
                let upstream_ch = target.channel_id;
                let target_bytes: &[u8] = target.maximum_target.inner_as_ref();
                let pool_difficulty = target_to_difficulty(target_bytes);

                if let Some(pair) = self
                    .channels
                    .values_mut()
                    .find(|m| m.upstream_channel_id == Some(upstream_ch))
                {
                    let prev_difficulty = pair.pool_difficulty.unwrap_or(0.0);
                    let pct_change = if prev_difficulty > 0.0 {
                        (pool_difficulty - prev_difficulty) / prev_difficulty * 100.0
                    } else {
                        0.0
                    };
                    let profile_elapsed = pair.gate.elapsed_secs();
                    let supply = pair.gate.current_supply_spm();
                    let target_spm = pair.gate.current_target_spm();
                    info!(
                        upstream_ch,
                        new_difficulty = pool_difficulty,
                        prev_difficulty = prev_difficulty,
                        pct_change = format!("{:+.1}%", pct_change),
                        profile_elapsed_secs = format!("{:.1}", profile_elapsed),
                        supply_spm = format!("{:.2}", supply),
                        target_spm = format!("{:.2}", target_spm),
                        "Pool vardiff fired"
                    );
                    pair.pool_difficulty = Some(pool_difficulty);
                }

                if self.config.min_downstream_difficulty > 0.0 {
                    self.handle_set_target_with_floor(upstream_ch, target.clone().into_static())
                        .await;
                } else {
                    if let Some(pair) = self
                        .channels
                        .values_mut()
                        .find(|m| m.upstream_channel_id == Some(upstream_ch))
                    {
                        pair.miner_difficulty = pool_difficulty;
                        debug!(
                            upstream_ch,
                            difficulty = pool_difficulty,
                            "SetTarget: forwarding to miner"
                        );
                    }
                    self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                        .await;
                }
            }
            Mining::SetExtranoncePrefix(ref prefix) => {
                let upstream_ch = prefix.channel_id;
                self.forward_to_downstream_by_upstream_channel(upstream_ch, msg)
                    .await;
            }
            Mining::SubmitSharesSuccess(ref s) => {
                debug!(
                    channel_id = s.channel_id,
                    last_seq = s.last_sequence_number,
                    accepted = s.new_submits_accepted_count,
                    "Pool acknowledged shares"
                );
            }
            Mining::SubmitSharesError(ref e) => {
                warn!(
                    channel_id = e.channel_id,
                    seq = e.sequence_number,
                    error = %e.error_code.as_utf8_or_hex(),
                    "Pool rejected share"
                );
            }
            other => {
                debug!("Upstream mining message (unhandled): {other}");
            }
        }
    }

    async fn handle_set_target_with_floor(
        &mut self,
        upstream_channel_id: u32,
        set_target: stratum_apps::stratum_core::mining_sv2::SetTarget<'static>,
    ) {
        let pair = self
            .channels
            .values_mut()
            .find(|m| m.upstream_channel_id == Some(upstream_channel_id));

        let Some(pair) = pair else {
            debug!(
                upstream_channel_id,
                "No mapping for SetTarget (floor check)"
            );
            return;
        };

        // Convert floor difficulty to a max target: target = 2^256 / difficulty
        // For U256 target representation, higher value = easier.
        // Pool's target is in set_target.maximum_target (le bytes).
        let floor_difficulty = self.config.min_downstream_difficulty;

        // Compare: is pool's target easier (larger) than the floor target?
        let pool_target_bytes: &[u8] = set_target.maximum_target.inner_as_ref();
        let pool_target_difficulty = target_to_difficulty(pool_target_bytes);

        if pool_target_difficulty < floor_difficulty {
            // Pool's difficulty is below floor -- override with floor.
            pair.floor_active = true;
            debug!(
                upstream_channel_id,
                pool_diff = pool_target_difficulty,
                floor_diff = floor_difficulty,
                "Floor active: pool difficulty below floor, not forwarding SetTarget"
            );
            // Don't forward -- miner keeps its current (harder) target.
            // miner_difficulty stays unchanged.
        } else {
            // Pool's difficulty is at or above floor -- forward normally.
            pair.floor_active = false;
            pair.miner_difficulty = pool_target_difficulty;
            debug!(
                upstream_channel_id,
                difficulty = pool_target_difficulty,
                "SetTarget: forwarding to miner (via floor handler)"
            );
            let ds_id = pair.downstream_id;
            self.send_to_downstream(ds_id, Message::Mining(Mining::SetTarget(set_target)))
                .await;
        }
    }

    async fn forward_to_downstream_by_upstream_channel(
        &mut self,
        upstream_channel_id: u32,
        msg: Mining<'static>,
    ) {
        let pair = self
            .channels
            .values()
            .find(|m| m.upstream_channel_id == Some(upstream_channel_id));

        if let Some(pair) = pair {
            let ds_id = pair.downstream_id;
            self.send_to_downstream(ds_id, Message::Mining(msg)).await;
        } else {
            // No exact match — broadcast to all connected downstreams.
            // This handles group-channel messages (channel_id 0) and cases where
            // the pool uses a channel_id the proxy hasn't mapped yet.
            let downstream_ids: Vec<_> = self.downstreams.keys().copied().collect();
            for ds_id in downstream_ids {
                self.send_to_downstream(ds_id, Message::Mining(msg.clone()))
                    .await;
            }
        }
    }

    async fn send_to_downstream(&mut self, id: DownstreamId, msg: Message) {
        let frame: StandardSv2Frame<Message> = match msg.try_into() {
            Ok(f) => f,
            Err(e) => {
                warn!(
                    downstream_id = id,
                    "Failed to encode message for downstream: {e:?}"
                );
                return;
            }
        };

        if let Some(ds) = self.downstreams.get_mut(&id) {
            if let Err(e) = ds.writer.write_frame(frame.into()).await {
                warn!(downstream_id = id, "Failed to write to downstream: {e:?}");
                self.downstreams.remove(&id);
            }
        }
    }
}

fn target_to_difficulty(target_le: &[u8]) -> f64 {
    let mut msb_index = 31;
    while msb_index > 0 && target_le[msb_index] == 0 {
        msb_index -= 1;
    }
    if target_le[msb_index] == 0 {
        return f64::MAX;
    }

    let mut target_val: f64 = 0.0;
    for i in (0..=msb_index).rev() {
        target_val = target_val * 256.0 + target_le[i] as f64;
    }

    if target_val == 0.0 {
        return f64::MAX;
    }

    // 2^256 / target via log to avoid overflow
    let log_diff = 256.0 * 2.0_f64.ln() - target_val.ln();
    log_diff.exp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_to_difficulty_known_values() {
        // All zeros → max difficulty
        let zeros = [0u8; 32];
        assert_eq!(target_to_difficulty(&zeros), f64::MAX);

        // All 0xFF → difficulty ~1 (target ≈ 2^256 - 1)
        let max_target = [0xFF; 32];
        let diff = target_to_difficulty(&max_target);
        assert!(diff > 0.9 && diff < 1.1, "Expected ~1.0, got {diff}");

        // Target = 1 (LE: [1, 0, 0, ...]) → difficulty = 2^256
        let mut one = [0u8; 32];
        one[0] = 1;
        let diff = target_to_difficulty(&one);
        let expected = (256.0 * 2.0_f64.ln()).exp(); // 2^256
        let ratio = diff / expected;
        assert!(
            ratio > 0.99 && ratio < 1.01,
            "Expected 2^256, ratio={ratio}"
        );
    }
}
