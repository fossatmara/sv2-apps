use std::{convert::TryFrom, sync::atomic::Ordering};

use stratum_apps::stratum_core::{
    binary_sv2::Str0255,
    bitcoin::Target,
    channels_sv2::{
        server::{
            error::{ExtendedChannelError, StandardChannelError},
            extended::ExtendedChannel,
            jobs::job_store::DefaultJobStore,
            share_accounting::{ShareValidationError, ShareValidationResult},
            standard::StandardChannel,
        },
        target::hash_rate_to_target,
        Vardiff, VardiffState,
    },
    extensions_sv2::{
        UserIdentity, EXTENSION_TYPE_WORKER_HASHRATE_TRACKING, TLV_FIELD_TYPE_USER_IDENTITY,
    },
    handlers_sv2::{HandleMiningMessagesFromClientAsync, SupportedChannelTypes},
    mining_sv2::*,
    parsers_sv2::{Mining, TemplateDistribution, Tlv, TlvField},
    template_distribution_sv2::SubmitSolution,
};
use tracing::{error, info};

use jd_server_sv2::job_declarator::SetCustomMiningJobResponse;

// ===========================================================================
// Telemetry-hint guard for UpdateChannel (Home A — see
// stratum sim/docs/NOMINAL_HASHRATE_COLDSTART.md "The guard, as it will ship").
//
// SAFETY FIX, not only a feature: today this handler calls update_channel
// UNCONDITIONALLY, so it tightens the operating point on the miner's unverified
// say-so on every upward revision — the unguarded-upward injection that is the
// over-difficulty spiral entry. The guard replaces that with the eager-ease/
// reluctant-tighten asymmetry: act on a plausible DOWNWARD revision (the safe,
// self-healing direction), DEFER an upward revision to the share-driven vardiff
// loop (which owns tightening on corroborated evidence). The asymmetry is
// decided by worst-case survivability under a missing protocol field: an
// UpdateChannel carries no device count, so the pool cannot tell a legitimate
// aggregate-attach upward revision from an unbacked say-so claim — and easing
// on a false hint costs only a bounded, self-correcting share burst, while
// tightening on one is the spiral.
// ===========================================================================

/// Smallest declared nominal (H/s) the guard will act on. Below this a
/// declaration is treated as a sentinel/garbage (the `nominal = 1` case observed
/// in the field) and ignored. A STATIC floor — no share-rate dependence, so it
/// is well-defined at all channel states (cold, post-reset, mid-run).
const MIN_PLAUSIBLE_NOMINAL_HS: f32 = 1_000.0;

/// Pool difficulty floor as a hashrate (H/s). The downward ease is clamped so it
/// cannot drop the operating point below this. SHIPS NON-BITING: there is no
/// pool difficulty-band policy in config today (only vardiff's internal
/// DEFAULT_MIN_HASHRATE = 1.0), so a real floor would change live vardiff
/// behavior on the reference baseline and must be an explicit operator choice.
/// At 1.0 H/s this clamp is wired-and-ready but vacuous; the over-low-downward
/// case is therefore bounded only by the miner's own max_target until an
/// operator sets a real band. (See the doc's "pool-floor operand" gap.)
const POOL_FLOOR_HASHRATE: f32 = 1.0;

/// The guard's verdict for one UpdateChannel revision.
#[derive(Debug, PartialEq)]
enum HintAction {
    /// Plausible downward revision: ease the operating point to this nominal
    /// (already the lower-of declared, the caller still clamps to max_target).
    EaseDown(f32),
    /// Plausible upward revision: do NOT tighten on the nominal; defer to the
    /// share loop. A max_target shrink in the same message is still honored.
    DeferUp,
    /// Implausible declaration (sentinel/garbage): ignore the nominal entirely.
    /// A max_target shrink is honored ONLY if the max_target is itself sane.
    Reject,
}

/// Classify an UpdateChannel revision under the eager-ease/reluctant-tighten
/// asymmetry. `current_nominal` is the channel's present operating point
/// (get_nominal_hashrate); `declared` is msg.nominal_hash_rate.
///
/// BOTH operands are screened — the direction comparison `declared <
/// current_nominal` is only as trustworthy as both sides. `current_nominal` is
/// NOT guaranteed finite-positive by construction: it is the fire-path-written
/// register, seeded at open from the declared open nominal via
/// `hash_rate_to_target`, and that converter rejects ONLY negative-hashrate /
/// zero-spm — it accepts 0.0, NaN and +inf (they cast to a valid `u128`), and
/// the pool's open handler does not screen the nominal either. So a channel can
/// legitimately exist with a degenerate `current_nominal`; treating it as a
/// clean reference would mis-classify (a plausible `declared` reads "downward"
/// against a garbage-high reference → unwarranted ease, or "upward" against a
/// garbage-low one → wrong defer).
///
/// ORDER IS LOAD-BEARING — the `!declared.is_finite()` check MUST stay first:
/// NaN reaches `<` as silently-false and would fall through to `DeferUp`. Do not
/// reorder the magnitude check ahead of it.
fn classify_hint(declared: f32, current_nominal: f32) -> HintAction {
    if !declared.is_finite() || declared < MIN_PLAUSIBLE_NOMINAL_HS {
        // the DECLARATION itself is garbage → ignore in full.
        HintAction::Reject
    } else if !current_nominal.is_finite() || current_nominal < MIN_PLAUSIBLE_NOMINAL_HS {
        // declaration is plausible, but the REFERENCE is degenerate → no
        // trustworthy direction. Can't justify an ease (the dangerous-adjacent
        // leg) against a garbage reference, so defer to the share loop (the safe
        // leg). declared is already known-good here (screened above).
        HintAction::DeferUp
    } else if declared < current_nominal {
        HintAction::EaseDown(declared)
    } else {
        HintAction::DeferUp
    }
}

/// The pool-floor target for a given share rate: the easiest target (highest
/// value) the pool will run. The ease clamps to `min(miner_max, this)` — in
/// target space `.min` picks the harder ceiling, so this bounds how far an ease
/// can lower difficulty. Non-biting at POOL_FLOOR_HASHRATE = 1.0 today.
fn pool_floor_target(shares_per_minute: f32) -> Option<Target> {
    hash_rate_to_target(POOL_FLOOR_HASHRATE as f64, shares_per_minute as f64)
        .ok()
        .map(Target::from)
}

// NOTE on the REJECT branch and max_target: the spec mandates reflecting a
// max_target SHRINK, and the plausible branches (ease/defer) preserve that by
// passing max_target through update_channel. The reject branch does NOT honor
// the message's max_target — but the load-bearing reason is NOT "the shrink is
// implausible because the nominal is" (nominal_hash_rate and maximum_target are
// INDEPENDENT protocol fields, populated by different firmware paths; a miner
// with a broken telemetry path could send a sentinel nominal AND a legitimate
// shrink). The reason is that the shrink is UNSCREENABLE: a max_target shrink
// moves difficulty UP (toward the ceiling), and a sentinel-tight shrink is a
// difficulty-slam DoS whose only screen is a pool difficulty CEILING — which
// does not exist in config (same missing-operand gap as the floor). Performing
// an unvalidatable, side-effectful request from an already-rejected message is
// the wrong default, so we drop the whole message. The spec assumes the pool
// CAN evaluate the shrink; it does not contemplate "the shrink is itself an
// attack I have no policy to bound." REVERSIBILITY (same discipline as the
// pool-floor and warm-test gaps): once a pool difficulty band/ceiling is in
// config, the reject branch can honor a shrink screened against the ceiling, and
// this deviation closes — it is scoped to the missing operand, not to a claim
// about miner behavior.

use crate::{
    channel_manager::{ChannelManager, RouteMessageTo, CLIENT_SEARCH_SPACE_BYTES},
    error::{self, PoolError, PoolErrorKind},
    utils::{create_close_channel_msg, PayoutMode, PayoutModeError},
};

#[cfg_attr(not(test), hotpath::measure_all)]
impl HandleMiningMessagesFromClientAsync for ChannelManager {
    type Error = PoolError<error::ChannelManager>;

    fn get_channel_type_for_client(&self, _client_id: Option<usize>) -> SupportedChannelTypes {
        SupportedChannelTypes::GroupAndExtended
    }

    fn is_work_selection_enabled_for_client(&self, _client_id: Option<usize>) -> bool {
        true
    }

    fn is_client_authorized(
        &self,
        _client_id: Option<usize>,
        _user_identity: &Str0255,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }

    fn get_negotiated_extensions_with_client(
        &self,
        client_id: Option<usize>,
    ) -> Result<Vec<u16>, Self::Error> {
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");
        self.channel_manager_data.super_safe_lock(|data| {
            let Some(downstream) = data.downstream.get(&downstream_id) else {
                return Err(PoolError::disconnect(
                    PoolErrorKind::DownstreamNotFound(downstream_id),
                    downstream_id,
                ));
            };
            downstream
                .downstream_data
                .super_safe_lock(|data| Ok(data.negotiated_extensions.clone()))
        })
    }

    async fn handle_close_channel(
        &mut self,
        client_id: Option<usize>,
        msg: CloseChannel<'_>,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        info!("Received Close Channel: {msg}");
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");
        self.channel_manager_data
            .super_safe_lock(|channel_manager_data| {
                let Some(downstream) = channel_manager_data.downstream.get_mut(&downstream_id)
                else {
                    return Err(PoolError::disconnect(
                        PoolErrorKind::DownstreamNotFound(downstream_id),
                        downstream_id,
                    ));
                };

                downstream
                    .downstream_data
                    .super_safe_lock(|downstream_data| {
                        downstream_data.standard_channels.remove(&msg.channel_id);
                        downstream_data.extended_channels.remove(&msg.channel_id);
                    });
                channel_manager_data
                    .vardiff
                    .remove(&(downstream_id, msg.channel_id).into());
                Ok(())
            })
    }

    async fn handle_open_standard_mining_channel(
        &mut self,
        client_id: Option<usize>,
        msg: OpenStandardMiningChannel<'_>,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        let request_id = msg.get_request_id_as_u32();
        let user_identity = msg.user_identity.as_utf8_or_hex();
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");

        info!("Received OpenStandardMiningChannel: {}", msg);

        let messages = self.channel_manager_data.super_safe_lock(|channel_manager_data| {
            let Some(downstream) = channel_manager_data.downstream.get_mut(&downstream_id) else {
                return Err(PoolError::disconnect(PoolErrorKind::DownstreamIdNotFound, downstream_id));
            };

            if downstream.requires_custom_work.load(Ordering::SeqCst) {
                error!("OpenStandardMiningChannel: Standard Channels are not supported for this connection");
                let open_standard_mining_channel_error = OpenMiningChannelError {
                    request_id,
                    error_code: ERROR_CODE_OPEN_MINING_CHANNEL_STANDARD_CHANNELS_NOT_SUPPORTED_FOR_CUSTOM_WORK
                        .to_string()
                        .try_into()
                        .expect("error code must be valid string"),
                };
                return Ok(vec![(downstream_id, Mining::OpenMiningChannelError(open_standard_mining_channel_error)).into()]);
            }

            let Some(last_future_template) = channel_manager_data.last_future_template.clone() else {
                return Err(PoolError::disconnect(PoolErrorKind::FutureTemplateNotPresent, downstream_id));
            };

            let Some(last_set_new_prev_hash_tdp) = channel_manager_data.last_new_prev_hash.clone() else {
                return Err(PoolError::disconnect(PoolErrorKind::LastNewPrevhashNotFound, downstream_id));
            };

            let payout_mode = match PayoutMode::try_from(user_identity.as_str()) {
                Ok(mode) => mode,
                Err(PayoutModeError::NoPayoutMode(_)) => PayoutMode::FullDonation,
                Err(_) => {
                    error!("Invalid user_identity '{}': does not match any supported identity format", user_identity);
                    let open_standard_mining_channel_error = OpenMiningChannelError {
                        request_id,
                        error_code: ERROR_CODE_OPEN_MINING_CHANNEL_INVALID_USER_IDENTITY
                            .to_string()
                            .try_into()
                            .expect("error code must be valid string"),
                    };
                    return Ok(vec![(downstream_id, Mining::OpenMiningChannelError(open_standard_mining_channel_error)).into()]);
                }
            };

            let coinbase_outputs = payout_mode.coinbase_outputs(
                last_future_template.coinbase_tx_value_remaining,
                &self.coinbase_reward_script,
            );

            downstream.downstream_data.super_safe_lock(|downstream_data| {
                downstream_data.payout_mode = Some(payout_mode);

                let nominal_hash_rate = msg.nominal_hash_rate;
                let requested_max_target = Target::from_le_bytes(msg.max_target.inner_as_ref().try_into().unwrap());
                let extranonce_prefix = channel_manager_data.extranonce_allocator.allocate_standard().map_err(PoolError::shutdown)?;

                let channel_id = downstream_data.channel_id_factory.fetch_add(1, Ordering::SeqCst);
                let job_store = DefaultJobStore::new();

                let mut standard_channel = match StandardChannel::new_for_pool(channel_id, user_identity.to_string(), extranonce_prefix, requested_max_target, nominal_hash_rate, self.share_batch_size, self.shares_per_minute, job_store, self.pool_tag_string.clone()) {
                    Ok(channel) => channel,
                    Err(e) => match e {
                        StandardChannelError::OpenChannelInvalidNominalHashrate(code) => {
                            error!("OpenMiningChannelError: {}", code);
                            let open_standard_mining_channel_error = OpenMiningChannelError {
                                request_id,
                                error_code: code
                                    .to_string()
                                    .try_into()
                                    .expect("error code must be valid string"),
                            };
                            return Ok(vec![(downstream_id, Mining::OpenMiningChannelError(open_standard_mining_channel_error)).into()]);
                        }
                        _ => {
                            error!("error in handle_open_standard_mining_channel: {:?}", e);
                            return Err(PoolError::disconnect(PoolErrorKind::ChannelErrorSender, downstream_id) );
                        }
                    },
                };

                let group_channel_id = downstream_data.group_channel.get_group_channel_id();
                let extranonce_prefix_size = standard_channel.get_extranonce_prefix().len();

                let open_standard_mining_channel_success = OpenStandardMiningChannelSuccess {
                    request_id: msg.request_id,
                    channel_id,
                    target: standard_channel.get_target().to_le_bytes().into(),
                    extranonce_prefix: standard_channel.get_extranonce_prefix().to_vec().try_into().expect("Extranonce_prefix must be valid"),
                    group_channel_id
                }.into_static();

                let mut  messages: Vec<RouteMessageTo> = Vec::new();

                messages.push((downstream_id, Mining::OpenStandardMiningChannelSuccess(open_standard_mining_channel_success)).into());

                let template_id = last_future_template.template_id;

                // create a future standard job based on the last future template
                standard_channel.on_new_template(last_future_template, coinbase_outputs.clone()).map_err(PoolError::shutdown)?;
                let future_standard_job_id = standard_channel
                    .get_future_job_id_from_template_id(template_id)
                    .expect("future job id must exist");
                let future_standard_job = standard_channel
                    .get_future_job(future_standard_job_id)
                    .expect("future job must exist");
                let future_standard_job_message =
                    future_standard_job.get_job_message().clone().into_static();

                messages.push((downstream_id, Mining::NewMiningJob(future_standard_job_message)).into());
                let prev_hash = last_set_new_prev_hash_tdp.prev_hash.clone();
                let header_timestamp = last_set_new_prev_hash_tdp.header_timestamp;
                let n_bits = last_set_new_prev_hash_tdp.n_bits;
                let set_new_prev_hash_mining = SetNewPrevHash {
                    channel_id,
                    job_id: future_standard_job_id,
                    prev_hash,
                    min_ntime: header_timestamp,
                    nbits: n_bits,
                };

                standard_channel
                .on_set_new_prev_hash(last_set_new_prev_hash_tdp.clone()).map_err(PoolError::shutdown)?;

                messages.push((downstream_id, Mining::SetNewPrevHash(set_new_prev_hash_mining)).into());

                downstream_data.standard_channels.insert(channel_id, standard_channel);
                if !downstream.requires_standard_jobs.load(Ordering::SeqCst) {
                    downstream_data.group_channel.add_channel_id(channel_id, extranonce_prefix_size).map_err(|e| {
                        error!("Failed to add channel id to group channel: {:?}", e);
                        PoolError::shutdown(e)
                    })?;
                }
                let vardiff = VardiffState::new().map_err(PoolError::shutdown)?;
                channel_manager_data.vardiff.insert((downstream_id, channel_id).into(), vardiff);

                Ok(messages)
            })
        })?;

        for message in messages {
            // A send can only fail if the receiver side of the channel is closed.
            // Since this is an unbounded channel, it cannot fail due to capacity
            // limits (which would only apply to bounded channels).
            if let Err(e) = message.forward(&self.channel_manager_io).await {
                error!("Failed to forward message {e:?}");
            }
        }

        Ok(())
    }

    async fn handle_open_extended_mining_channel(
        &mut self,
        client_id: Option<usize>,
        msg: OpenExtendedMiningChannel<'_>,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        let request_id = msg.get_request_id_as_u32();
        let user_identity = msg.user_identity.as_utf8_or_hex();
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");
        info!("Received OpenExtendedMiningChannel: {}", msg);

        let nominal_hash_rate = msg.nominal_hash_rate;
        let requested_max_target =
            Target::from_le_bytes(msg.max_target.inner_as_ref().try_into().unwrap());
        let requested_min_rollable_extranonce_size = msg.min_extranonce_size;

        let messages = self
            .channel_manager_data
            .super_safe_lock(|channel_manager_data| {
                let Some(downstream) = channel_manager_data.downstream.get_mut(&downstream_id)
                else {
                    return Err(PoolError::disconnect(PoolErrorKind::DownstreamIdNotFound, downstream_id));
                };

                if downstream.requires_standard_jobs.load(Ordering::SeqCst) {
                    let open_extended_mining_channel_error = OpenMiningChannelError {
                        request_id,
                        error_code: ERROR_CODE_OPEN_MINING_CHANNEL_EXTENDED_CHANNELS_NOT_SUPPORTED_FOR_STANDARD_JOBS
                            .to_string()
                            .try_into()
                            .expect("error code must be valid string"),
                    };
                    return Ok(vec![(
                        downstream_id,
                        Mining::OpenMiningChannelError(
                            open_extended_mining_channel_error,
                        ),
                    )
                        .into()]);
                }

                downstream
                    .downstream_data
                    .super_safe_lock(|downstream_data| {
                        let mut messages: Vec<RouteMessageTo> = Vec::new();

                        let extranonce_prefix = match channel_manager_data
                            .extranonce_allocator
                            .allocate_extended(requested_min_rollable_extranonce_size.into())
                        {
                            Ok(prefix) => prefix,
                            Err(_) => {
                                error!("OpenMiningChannelError: min-extranonce-size-too-large");
                                let open_extended_mining_channel_error = OpenMiningChannelError {
                                    request_id,
                                    error_code: ERROR_CODE_OPEN_MINING_CHANNEL_MIN_EXTRANONCE_SIZE_TOO_LARGE
                                        .to_string()
                                        .try_into()
                                        .expect("error code must be valid string"),
                                };
                                return Ok(vec![(
                                    downstream_id,
                                    Mining::OpenMiningChannelError(
                                        open_extended_mining_channel_error,
                                    ),
                                )
                                    .into()]);
                            }
                        };

                        let payout_mode = match PayoutMode::try_from(user_identity.as_str()) {
                            Ok(mode) => mode,
                            Err(PayoutModeError::NoPayoutMode(_)) => PayoutMode::FullDonation,
                            Err(_) => {
                                error!("Invalid user_identity '{}': does not match any supported identity format", user_identity);
                                let open_extended_mining_channel_error = OpenMiningChannelError {
                                    request_id,
                                    error_code: ERROR_CODE_OPEN_MINING_CHANNEL_INVALID_USER_IDENTITY
                                        .to_string()
                                        .try_into()
                                        .expect("error code must be valid string"),
                                };
                                return Ok(vec![(
                                    downstream_id,
                                    Mining::OpenMiningChannelError(
                                        open_extended_mining_channel_error,
                                    ),
                                )
                                    .into()]);
                            }
                        };

                        downstream_data.payout_mode = Some(payout_mode.clone());

                        let channel_id = downstream_data
                            .channel_id_factory
                            .fetch_add(1, Ordering::SeqCst);
                        let job_store = DefaultJobStore::new();

                        let mut extended_channel = match ExtendedChannel::new_for_pool(
                            channel_id,
                            user_identity.to_string(),
                            extranonce_prefix,
                            requested_max_target,
                            nominal_hash_rate,
                            true, // version rolling always allowed
                            CLIENT_SEARCH_SPACE_BYTES as u16,
                            self.share_batch_size,
                            self.shares_per_minute,
                            job_store,
                            self.pool_tag_string.clone(),
                        ) {
                            Ok(channel) => channel,
                            Err(e) => {
                                match e {
                                ExtendedChannelError::OpenChannelInvalidNominalHashrate(code) => {
                                    error!("OpenMiningChannelError: {}", code);
                                    let open_extended_mining_channel_error =
                                        OpenMiningChannelError {
                                            request_id,
                                            error_code: code
                                                .to_string()
                                                .try_into()
                                                .expect("error code must be valid string"),
                                        };
                                    return Ok(vec![(
                                        downstream_id,
                                        Mining::OpenMiningChannelError(
                                            open_extended_mining_channel_error,
                                        ),
                                    )
                                        .into()]);
                                }
                                ExtendedChannelError::RequestedMinExtranonceSizeTooLarge(code) => {
                                    error!("OpenMiningChannelError: {}", code);
                                    let open_extended_mining_channel_error =
                                        OpenMiningChannelError {
                                            request_id,
                                            error_code: code
                                                .to_string()
                                                .try_into()
                                                .expect("error code must be valid string"),
                                        };
                                    return Ok(vec![(
                                        downstream_id,
                                        Mining::OpenMiningChannelError(
                                            open_extended_mining_channel_error,
                                        ),
                                    )
                                        .into()]);
                                }
                                e => {
                                    error!("error in handle_open_extended_mining_channel: {:?}", e);
                                    return Err(PoolError::disconnect(e, downstream_id))?;
                                }
                                }
                            },
                        };

                        let group_channel_id = downstream_data.group_channel.get_group_channel_id();

                        let open_extended_mining_channel_success =
                            OpenExtendedMiningChannelSuccess {
                                request_id,
                                channel_id,
                                target: extended_channel.get_target().to_le_bytes().into(),
                                extranonce_prefix: extended_channel
                                    .get_extranonce_prefix()
                                    .to_vec()
                                    .try_into().map_err(PoolError::shutdown)?,
                                extranonce_size: extended_channel.get_rollable_extranonce_size(),
                                group_channel_id,
                            }
                            .into_static();
                        info!("Sending OpenExtendedMiningChannel.Success (downstream_id: {downstream_id}): {open_extended_mining_channel_success}");

                        messages.push(
                            (
                                downstream_id,
                                Mining::OpenExtendedMiningChannelSuccess(
                                    open_extended_mining_channel_success,
                                ),
                            )
                                .into(),
                        );

                        let Some(last_set_new_prev_hash_tdp) =
                            channel_manager_data.last_new_prev_hash.clone()
                        else {
                            return Err(PoolError::disconnect(PoolErrorKind::LastNewPrevhashNotFound, downstream_id));
                        };

                        let Some(last_future_template) =
                            channel_manager_data.last_future_template.clone()
                        else {
                            return Err(PoolError::disconnect(PoolErrorKind::FutureTemplateNotPresent,downstream_id));
                        };

                        // if the client requires custom work, we don't need to send any extended
                        // jobs so we just process the SetNewPrevHash
                        // message
                        if downstream.requires_custom_work.load(Ordering::SeqCst) {
                            extended_channel.on_set_new_prev_hash(last_set_new_prev_hash_tdp).map_err(PoolError::shutdown)?;
                            // if the client does not require custom work, we need to send the
                            // future extended job
                            // and the SetNewPrevHash message
                        } else {
                            let coinbase_outputs = payout_mode.coinbase_outputs(
                                last_future_template.coinbase_tx_value_remaining,
                                &self.coinbase_reward_script,
                            );

                            extended_channel.on_new_template(
                                last_future_template.clone(),
                                coinbase_outputs,
                            ).map_err(PoolError::shutdown)?;

                            let future_extended_job_id = extended_channel
                                .get_future_job_id_from_template_id(last_future_template.template_id)
                                .expect("future job id must exist");
                            let future_extended_job = extended_channel
                                .get_future_job(future_extended_job_id)
                                .expect("future job must exist");

                            let future_extended_job_message =
                                future_extended_job.get_job_message().clone().into_static();

                            // send this future job as new job message
                            // to be immediately activated with the subsequent SetNewPrevHash
                            // message
                            messages.push(
                                (
                                    downstream_id,
                                    Mining::NewExtendedMiningJob(future_extended_job_message),
                                )
                                    .into(),
                            );

                            // SetNewPrevHash message activates the future job
                            let prev_hash = last_set_new_prev_hash_tdp.prev_hash.clone();
                            let header_timestamp = last_set_new_prev_hash_tdp.header_timestamp;
                            let n_bits = last_set_new_prev_hash_tdp.n_bits;
                            let set_new_prev_hash_mining = SetNewPrevHash {
                                channel_id,
                                job_id: future_extended_job_id,
                                prev_hash,
                                min_ntime: header_timestamp,
                                nbits: n_bits,
                            };

                            extended_channel.on_set_new_prev_hash(last_set_new_prev_hash_tdp).map_err(PoolError::shutdown)?;

                            messages.push(
                                (
                                    downstream_id,
                                    Mining::SetNewPrevHash(set_new_prev_hash_mining),
                                )
                                    .into(),
                            );

                            let full_extranonce_size = extended_channel.get_full_extranonce_size();
                            downstream_data.group_channel.add_channel_id(channel_id, full_extranonce_size).map_err(|e| {
                                error!("Failed to add channel id to group channel: {:?}", e);
                                PoolError::shutdown(e)
                            })?;
                        }

                        downstream_data
                            .extended_channels
                            .insert(channel_id, extended_channel);
                        let vardiff = VardiffState::new().map_err(PoolError::shutdown)?;
                        channel_manager_data
                            .vardiff
                            .insert((downstream_id, channel_id).into(), vardiff);

                        Ok(messages)
                    })
            })?;

        for message in messages {
            // A send can only fail if the receiver side of the channel is closed.
            // Since this is an unbounded channel, it cannot fail due to capacity
            // limits (which would only apply to bounded channels).
            if let Err(e) = message.forward(&self.channel_manager_io).await {
                error!("Failed to forward message {e:?}");
            }
        }
        Ok(())
    }

    async fn handle_submit_shares_standard(
        &mut self,
        client_id: Option<usize>,
        msg: SubmitSharesStandard,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        info!("Received SubmitSharesStandard: {msg}");
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");

        let messages = self.channel_manager_data.super_safe_lock(|channel_manager_data| {
            let channel_id = msg.channel_id;

            let Some(downstream) = channel_manager_data.downstream.get(&downstream_id) else {
                return Err(PoolError::disconnect(PoolErrorKind::DownstreamNotFound(downstream_id), downstream_id));
            };

            downstream.downstream_data.super_safe_lock(|downstream_data| {
                let mut messages: Vec<RouteMessageTo> = Vec::new();
                let Some(standard_channel) = downstream_data.standard_channels.get_mut(&channel_id) else {
                    let submit_shares_error = SubmitSharesError {
                        channel_id,
                        sequence_number: msg.sequence_number,
                        error_code: ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID
                            .to_string()
                            .try_into()
                            .expect("error code must be valid string"),
                    };
                    error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID);
                    return Ok(vec![(downstream_id, Mining::SubmitSharesError(submit_shares_error)).into()]);
                };

                let Some(vardiff) = channel_manager_data.vardiff.get_mut(&(downstream_id, channel_id).into()) else {
                    return Ok(vec![(downstream_id, Mining::CloseChannel(create_close_channel_msg(channel_id, ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID))).into()]);
                };

                let res = standard_channel.validate_share(msg.clone());
                vardiff.increment_shares_since_last_update();


                match res {
                    Ok(ShareValidationResult::Valid(share_hash)) => {
                        let share_accounting = standard_channel.get_share_accounting();
                        if share_accounting.should_acknowledge() {
                            let success = SubmitSharesSuccess {
                                channel_id,
                                last_sequence_number: share_accounting.get_last_share_sequence_number(),
                                new_submits_accepted_count: share_accounting.get_last_batch_accepted(),
                                new_shares_sum: share_accounting.get_last_batch_work_sum(),
                            };
                            info!("SubmitSharesStandard: {} ✅", success);
                            messages.push((downstream_id, Mining::SubmitSharesSuccess(success)).into());
                        } else {
                            let share_work = standard_channel.get_target().difficulty_float();
                            info!(
                                "SubmitSharesStandard: valid share | downstream_id: {}, channel_id: {}, sequence_number: {}, share_hash: {}, share_work: {} ✅",
                                downstream_id, channel_id, msg.sequence_number, share_hash, share_work
                            );
                        }

                    }
                    Ok(ShareValidationResult::BlockFound(share_hash, template_id, coinbase)) => {
                        info!("SubmitSharesStandard: 💰 Block Found!!! 💰{share_hash}");
                        // if we have a template id (i.e.: this was not a custom job)
                        // we can propagate the solution to the TP
                        if let Some(template_id) = template_id {
                            info!("SubmitSharesStandard: Propagating solution to the Template Provider.");
                            let solution = SubmitSolution {
                                template_id,
                                version: msg.version,
                                header_timestamp: msg.ntime,
                                header_nonce: msg.nonce,
                                coinbase_tx: coinbase.try_into().map_err(PoolError::shutdown)?,
                            };
                            messages.push(TemplateDistribution::SubmitSolution(solution).into());
                        }
                        let share_accounting = standard_channel.get_share_accounting();
                        let success = SubmitSharesSuccess {
                            channel_id,
                            last_sequence_number: share_accounting.get_last_share_sequence_number(),
                            new_submits_accepted_count: share_accounting.get_last_batch_accepted(),
                            new_shares_sum: share_accounting.get_last_batch_work_sum(),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesSuccess(success)).into());
                    }
                    Err(ShareValidationError::Invalid(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };

                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::Stale(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::InvalidJobId(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::DoesNotMeetTarget(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::DuplicateShare(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::VersionRollingNotAllowed(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(e) => {
                        return Err(PoolError::disconnect(e, downstream_id))?;
                    }
                }

                Ok(messages)
            })
        })?;

        for message in messages {
            // A send can only fail if the receiver side of the channel is closed.
            // Since this is an unbounded channel, it cannot fail due to capacity
            // limits (which would only apply to bounded channels).
            if let Err(e) = message.forward(&self.channel_manager_io).await {
                error!("Failed to forward message {e:?}");
            }
        }

        Ok(())
    }

    async fn handle_submit_shares_extended(
        &mut self,
        client_id: Option<usize>,
        msg: SubmitSharesExtended<'_>,
        tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        info!("Received SubmitSharesExtended: {msg}");
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");

        // Extract user_identity from TLV fields if the extension is negotiated
        let negotiated_extensions = self.get_negotiated_extensions_with_client(client_id);
        let user_identity = if negotiated_extensions
            .as_ref()
            .is_ok_and(|exts| exts.contains(&EXTENSION_TYPE_WORKER_HASHRATE_TRACKING))
        {
            tlv_fields.and_then(|tlvs| {
                tlvs.iter()
                    .find(|tlv| {
                        tlv.r#type.extension_type == EXTENSION_TYPE_WORKER_HASHRATE_TRACKING
                            && tlv.r#type.field_type == TLV_FIELD_TYPE_USER_IDENTITY
                    })
                    .and_then(|tlv| UserIdentity::from_tlv(tlv).ok())
            })
        } else {
            None
        };

        let messages = self.channel_manager_data.super_safe_lock(|channel_manager_data| {
            let channel_id = msg.channel_id;
            let Some(downstream) = channel_manager_data.downstream.get(&downstream_id) else {
                return Err(PoolError::disconnect(PoolErrorKind::DownstreamNotFound(downstream_id), downstream_id));
            };

            downstream.downstream_data.super_safe_lock(|downstream_data| {
                let mut messages: Vec<RouteMessageTo> = Vec::new();
                let Some(extended_channel) = downstream_data.extended_channels.get_mut(&channel_id) else {
                    let error = SubmitSharesError {
                        channel_id,
                        sequence_number: msg.sequence_number,
                        error_code: ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID
                            .to_string()
                            .try_into()
                            .expect("error code must be valid string"),
                    };
                    error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID);
                    return Ok(vec![(downstream_id, Mining::SubmitSharesError(error)).into()]);
                };

                if let Some(_user_identity) = user_identity {
                    // here we have the UserIdentity TLV, so we can use it to enhance monitoring of individual miners in the future
                }

                let Some(vardiff) = channel_manager_data.vardiff.get_mut(&(downstream_id, channel_id).into()) else {
                    return Ok(vec![(downstream_id, Mining::CloseChannel(create_close_channel_msg(channel_id, ERROR_CODE_SUBMIT_SHARES_INVALID_CHANNEL_ID))).into()]);
                };

                let res = extended_channel.validate_share(msg.clone());
                vardiff.increment_shares_since_last_update();

                match res {
                    Ok(ShareValidationResult::Valid(share_hash)) => {
                        let share_accounting = extended_channel.get_share_accounting();
                        if share_accounting.should_acknowledge() {
                            let success = SubmitSharesSuccess {
                                channel_id,
                                last_sequence_number: share_accounting.get_last_share_sequence_number(),
                                new_submits_accepted_count: share_accounting.get_last_batch_accepted(),
                                new_shares_sum: share_accounting.get_last_batch_work_sum(),
                            };
                            info!("SubmitSharesExtended: {} ✅", success);
                            messages.push((downstream_id, Mining::SubmitSharesSuccess(success)).into());
                        } else {
                            let share_work = extended_channel.get_target().difficulty_float();
                            info!(
                                "SubmitSharesExtended: valid share | downstream_id: {}, channel_id: {}, sequence_number: {}, share_hash: {}, share_work: {} ✅",
                                downstream_id, channel_id, msg.sequence_number, share_hash, share_work
                            );
                        }
                    }
                    Ok(ShareValidationResult::BlockFound(share_hash, template_id, coinbase)) => {
                        info!("SubmitSharesExtended: 💰 Block Found!!! 💰{share_hash}");
                        // if we have a template id (i.e.: this was not a custom job)
                        // we can propagate the solution to the TP
                        if let Some(template_id) = template_id {
                            info!("SubmitSharesExtended: Propagating solution to the Template Provider.");
                            let solution = SubmitSolution {
                                template_id,
                                version: msg.version,
                                header_timestamp: msg.ntime,
                                header_nonce: msg.nonce,
                                coinbase_tx: coinbase.try_into().map_err(PoolError::shutdown)?,
                            };
                            messages.push(TemplateDistribution::SubmitSolution(solution).into());
                        }
                        let share_accounting = extended_channel.get_share_accounting();
                        let success = SubmitSharesSuccess {
                            channel_id,
                            last_sequence_number: share_accounting.get_last_share_sequence_number(),
                            new_submits_accepted_count: share_accounting.get_last_batch_accepted(),
                            new_shares_sum: share_accounting.get_last_batch_work_sum(),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesSuccess(success)).into());
                    }
                    Err(ShareValidationError::Invalid(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::Stale(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::InvalidJobId(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::DoesNotMeetTarget(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::DuplicateShare(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::BadExtranonceSize(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(ShareValidationError::VersionRollingNotAllowed(code)) => {
                        error!("SubmitSharesError: downstream_id: {}, channel_id: {}, sequence_number: {}, error_code: {} ❌", downstream_id, channel_id, msg.sequence_number, code);
                        let error = SubmitSharesError {
                            channel_id: msg.channel_id,
                            sequence_number: msg.sequence_number,
                            error_code: code
                                .to_string()
                                .try_into()
                                .expect("error code must be valid string"),
                        };
                        messages.push((downstream_id, Mining::SubmitSharesError(error)).into());
                    }
                    Err(e) => {
                        return Err(PoolError::disconnect(e, downstream_id))?;
                    }
                }

                Ok(messages)
            })
        })?;

        for message in messages {
            // A send can only fail if the receiver side of the channel is closed.
            // Since this is an unbounded channel, it cannot fail due to capacity
            // limits (which would only apply to bounded channels).
            if let Err(e) = message.forward(&self.channel_manager_io).await {
                error!("Failed to forward message {e:?}");
            }
        }

        Ok(())
    }

    async fn handle_update_channel(
        &mut self,
        client_id: Option<usize>,
        msg: UpdateChannel<'_>,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        info!("Received: {}", msg);

        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");

        let messages: Vec<RouteMessageTo> =
            self.channel_manager_data
                .super_safe_lock(|channel_manager_data| {
                    let Some(downstream) = channel_manager_data.downstream.get(&downstream_id)
                    else {
                        return Err(PoolError::disconnect(
                            PoolErrorKind::DownstreamNotFound(downstream_id),
                            downstream_id,
                        ));
                    };

                    downstream
                        .downstream_data
                        .super_safe_lock(|downstream_data| {
                            let mut messages = Vec::new();
                            let channel_id = msg.channel_id;
                            let new_nominal_hash_rate = msg.nominal_hash_rate;
                            let requested_maximum_target = Target::from_le_bytes(
                                msg.maximum_target.inner_as_ref().try_into().unwrap(),
                            );

                            if let Some(standard_channel) =
                                downstream_data.standard_channels.get_mut(&channel_id)
                            {
                                // GUARD: eager-ease / reluctant-tighten on the hint.
                                let spm = standard_channel.get_shares_per_minute();
                                let action = classify_hint(
                                    new_nominal_hash_rate,
                                    standard_channel.get_nominal_hashrate(),
                                );
                                let emit_set_target = match action {
                                    HintAction::EaseDown(eased_nominal) => {
                                        // clamp the ease to min(miner_max, pool_floor).
                                        let clamp = match pool_floor_target(spm) {
                                            Some(floor) => requested_maximum_target.min(floor),
                                            None => requested_maximum_target,
                                        };
                                        let res = standard_channel
                                            .update_channel(eased_nominal, Some(clamp));
                                        if let Err(e) = res {
                                            error!("UpdateChannelError: {:?}", e);
                                            match e {
                                                StandardChannelError::UpdateChannelInvalidNominalHashrate(code) => {
                                                    let update_channel_error = UpdateChannelError {
                                                        channel_id,
                                                        error_code: code.to_string().try_into()
                                                            .expect("error code must be valid string"),
                                                    };
                                                    messages.push((downstream_id,
                                                        Mining::UpdateChannelError(update_channel_error)).into());
                                                }
                                                _ => unreachable!(),
                                            }
                                        }
                                        true
                                    }
                                    HintAction::DeferUp => {
                                        // Do NOT tighten on the nominal — the share loop owns
                                        // tightening. Still honor a max_target SHRINK (spec):
                                        // re-apply with the CURRENT nominal so only the target
                                        // ceiling moves, never the operating point upward.
                                        if requested_maximum_target < *standard_channel.get_target() {
                                            let cur = standard_channel.get_nominal_hashrate();
                                            let _ = standard_channel
                                                .update_channel(cur, Some(requested_maximum_target));
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                    HintAction::Reject => {
                                        // Untrusted message — ignore in full (incl. its max_target).
                                        error!("UpdateChannel ignored: implausible nominal {} (< {} H/s sentinel floor)",
                                            new_nominal_hash_rate, MIN_PLAUSIBLE_NOMINAL_HS);
                                        false
                                    }
                                };
                                if emit_set_target {
                                    let new_target = standard_channel.get_target();
                                    let set_target = SetTarget {
                                        channel_id,
                                        maximum_target: new_target.to_le_bytes().into(),
                                    };
                                    messages.push((downstream_id, Mining::SetTarget(set_target)).into());
                                }
                            } else if let Some(extended_channel) =
                                downstream_data.extended_channels.get_mut(&channel_id)
                            {
                                // GUARD: eager-ease / reluctant-tighten on the hint.
                                let spm = extended_channel.get_shares_per_minute();
                                let action = classify_hint(
                                    new_nominal_hash_rate,
                                    extended_channel.get_nominal_hashrate(),
                                );
                                let emit_set_target = match action {
                                    HintAction::EaseDown(eased_nominal) => {
                                        let clamp = match pool_floor_target(spm) {
                                            Some(floor) => requested_maximum_target.min(floor),
                                            None => requested_maximum_target,
                                        };
                                        let res = extended_channel
                                            .update_channel(eased_nominal, Some(clamp));
                                        if let Err(e) = res {
                                            error!("UpdateChannelError: {:?}", e);
                                            match e {
                                                ExtendedChannelError::UpdateChannelInvalidNominalHashrate(code) => {
                                                    let update_channel_error = UpdateChannelError {
                                                        channel_id,
                                                        error_code: code.to_string().try_into()
                                                            .expect("error code must be valid string"),
                                                    };
                                                    messages.push((downstream_id,
                                                        Mining::UpdateChannelError(update_channel_error)).into());
                                                }
                                                _ => unreachable!(),
                                            }
                                        }
                                        true
                                    }
                                    HintAction::DeferUp => {
                                        if requested_maximum_target < *extended_channel.get_target() {
                                            let cur = extended_channel.get_nominal_hashrate();
                                            let _ = extended_channel
                                                .update_channel(cur, Some(requested_maximum_target));
                                            true
                                        } else {
                                            false
                                        }
                                    }
                                    HintAction::Reject => {
                                        error!("UpdateChannel ignored: implausible nominal {} (< {} H/s sentinel floor)",
                                            new_nominal_hash_rate, MIN_PLAUSIBLE_NOMINAL_HS);
                                        false
                                    }
                                };
                                if emit_set_target {
                                    let new_target = extended_channel.get_target();
                                    let set_target = SetTarget {
                                        channel_id,
                                        maximum_target: new_target.to_le_bytes().into(),
                                    };
                                    messages.push((downstream_id, Mining::SetTarget(set_target)).into());
                                }
                            } else {
                                error!("UpdateChannelError: invalid-channel-id");
                                let update_channel_error = UpdateChannelError {
                                    channel_id,
                                    error_code: ERROR_CODE_UPDATE_CHANNEL_INVALID_CHANNEL_ID
                                        .to_string()
                                        .try_into()
                                        .expect("error code must be valid string"),
                                };
                                messages.push(
                                    (
                                        downstream_id,
                                        Mining::UpdateChannelError(update_channel_error),
                                    )
                                        .into(),
                                );
                            }

                            Ok(messages)
                        })
                })?;

        for message in messages {
            // A send can only fail if the receiver side of the channel is closed.
            // Since this is an unbounded channel, it cannot fail due to capacity
            // limits (which would only apply to bounded channels).
            if let Err(e) = message.forward(&self.channel_manager_io).await {
                error!("Failed to forward message {e:?}");
            }
        }

        Ok(())
    }

    async fn handle_set_custom_mining_job(
        &mut self,
        client_id: Option<usize>,
        msg: SetCustomMiningJob<'_>,
        _tlv_fields: Option<&[Tlv]>,
    ) -> Result<(), Self::Error> {
        info!("Received: {}", msg);
        let downstream_id =
            client_id.expect("client_id must be present for downstream_id extraction");

        let Some(ref mut job_declarator) = self.job_declarator else {
            let error = SetCustomMiningJobError {
                request_id: msg.request_id,
                channel_id: msg.channel_id,
                error_code: ERROR_CODE_SET_CUSTOM_MINING_JOB_JD_NOT_SUPPORTED
                    .to_string()
                    .try_into()
                    .expect("error code must be valid string"),
            };
            let message: RouteMessageTo =
                (downstream_id, Mining::SetCustomMiningJobError(error)).into();
            message
                .forward(&self.channel_manager_io)
                .await
                .map_err(|e| PoolError::disconnect(e, downstream_id))?;
            return Ok(());
        };

        let msg_static = msg.clone().into_static();

        // Step 1: Validate the custom job via JDS (token + job validation).
        let jds_response = job_declarator
            .handle_set_custom_mining_job(msg_static.clone(), _tlv_fields)
            .await
            .map_err(|e| PoolError::shutdown(PoolErrorKind::Jds(e.into())))?;

        if let SetCustomMiningJobResponse::Error(jds_err) = jds_response {
            let message: RouteMessageTo = (
                downstream_id,
                Mining::SetCustomMiningJobError(jds_err.into_static()),
            )
                .into();
            message
                .forward(&self.channel_manager_io)
                .await
                .map_err(|e| PoolError::disconnect(e, downstream_id))?;
            return Ok(());
        }

        // Step 2: JDS validated successfully — commit the job to the extended channel.
        let message: RouteMessageTo =
            self.channel_manager_data
                .super_safe_lock(|channel_manager_data| {
                    let Some(downstream) = channel_manager_data.downstream.get_mut(&downstream_id)
                    else {
                        return Err(PoolError::disconnect(
                            PoolErrorKind::DownstreamNotFound(downstream_id),
                            downstream_id,
                        ));
                    };

                    downstream
                        .downstream_data
                        .super_safe_lock(|downstream_data| {
                            let Some(extended_channel) = downstream_data
                                .extended_channels
                                .get_mut(&msg_static.channel_id)
                            else {
                                error!("SetCustomMiningJobError: invalid-channel-id");
                                let error = SetCustomMiningJobError {
                                    request_id: msg_static.request_id,
                                    channel_id: msg_static.channel_id,
                                    error_code: ERROR_CODE_SET_CUSTOM_MINING_JOB_INVALID_CHANNEL_ID
                                        .to_string()
                                        .try_into()
                                        .expect("error code must be valid string"),
                                };
                                return Ok(
                                    (downstream_id, Mining::SetCustomMiningJobError(error)).into()
                                );
                            };

                            let job_id = extended_channel
                                .on_set_custom_mining_job(msg_static.clone())
                                .map_err(|error| PoolError::disconnect(error, downstream_id))?;

                            let success = SetCustomMiningJobSuccess {
                                channel_id: msg_static.channel_id,
                                request_id: msg_static.request_id,
                                job_id,
                            };
                            Ok((downstream_id, Mining::SetCustomMiningJobSuccess(success)).into())
                        })
                })?;

        message
            .forward(&self.channel_manager_io)
            .await
            .map_err(|e| PoolError::disconnect(e, downstream_id))?;

        Ok(())
    }
}

#[cfg(test)]
mod hint_guard_tests {
    use super::{classify_hint, pool_floor_target, HintAction, MIN_PLAUSIBLE_NOMINAL_HS};

    // ---- the three branches (happy path) ----
    #[test]
    fn ease_down_on_plausible_downward_revision() {
        // declared plausible AND below the current operating point → eager-ease.
        assert_eq!(
            classify_hint(50_000.0, 100_000.0),
            HintAction::EaseDown(50_000.0)
        );
    }

    #[test]
    fn defer_up_on_plausible_upward_revision() {
        // declared plausible AND at/above current → defer to the share loop.
        assert_eq!(classify_hint(200_000.0, 100_000.0), HintAction::DeferUp);
    }

    #[test]
    fn reject_on_sentinel_nominal() {
        // the nominal=1 field sentinel, and anything below the floor.
        assert_eq!(classify_hint(1.0, 100_000.0), HintAction::Reject);
        assert_eq!(classify_hint(999.0, 100_000.0), HintAction::Reject);
    }

    // ---- the boundary: exactly MIN_PLAUSIBLE_NOMINAL_HS ----
    #[test]
    fn boundary_exactly_at_floor_is_accepted_not_rejected() {
        // Guard is `< floor` rejects, so floor itself is ACCEPTED. Pin this so a
        // refactor to `<=` (which would reject the floor) is caught at build.
        let at_floor = MIN_PLAUSIBLE_NOMINAL_HS; // 1000.0
        // at_floor below current → ease (NOT reject); confirms the boundary side.
        assert_eq!(
            classify_hint(at_floor, 2_000.0),
            HintAction::EaseDown(at_floor)
        );
        // just below the floor → reject.
        assert_eq!(classify_hint(at_floor - 0.1, 2_000.0), HintAction::Reject);
    }

    // ---- non-finite: the classic silent-pass. NaN < x is false, so WITHOUT
    // the is_finite() guard a NaN would fall past reject AND past the direction
    // comparison (also false vs NaN) into the wrong arm. Test it's caught. ----
    #[test]
    fn nan_nominal_is_rejected() {
        assert_eq!(classify_hint(f32::NAN, 100_000.0), HintAction::Reject);
    }

    #[test]
    fn infinite_nominal_is_rejected() {
        assert_eq!(classify_hint(f32::INFINITY, 100_000.0), HintAction::Reject);
        // -inf is below the floor by ordering too, but is_finite catches it first.
        assert_eq!(classify_hint(f32::NEG_INFINITY, 100_000.0), HintAction::Reject);
    }

    // ---- clamp-direction: the seam most likely to silently invert.
    // In target space LOWER difficulty = LARGER target, so the clamp
    // `min(miner_max, pool_floor)` must pick the SMALLER target (= HARDER
    // difficulty). Pin that with two targets ordered unambiguously BY VALUE —
    // NOT via Target::MAX/ZERO: Target::MAX is the Bitcoin max-target,
    // numerically SMALL, and a vardiff floor target can exceed it (the first
    // draft of this test wrongly assumed MAX = largest and caught its own bad
    // premise — recorded here so it isn't re-introduced). Catches a future
    // `.min()` -> `.max()` inversion in the guard. ----
    #[test]
    fn ease_clamp_picks_the_harder_ceiling() {
        use stratum_apps::stratum_core::bitcoin::Target;
        let harder = Target::from_le_bytes([0x11u8; 32]); // smaller value
        let easier = Target::from_le_bytes([0xEEu8; 32]); // larger value
        assert!(harder < easier, "sanity: smaller target value = harder difficulty");
        assert_eq!(easier.min(harder), harder, ".min must pick the harder (smaller) ceiling");
        assert_eq!(harder.min(easier), harder, ".min result is order-independent");
        // the guard's pool_floor_target is well-formed for a real spm.
        assert!(pool_floor_target(6.0).is_some());
    }

    // ---- reject is side-effect-free BY CONSTRUCTION: the Reject arm calls no
    // channel mutator (only error!() + emit_set_target=false). This test pins the
    // classifier verdict that drives that arm; the no-op property is then a
    // structural property of the arm (no update_channel / set_nominal call),
    // verified by inspection against source. ----
    // ---- the REFERENCE operand (current_nominal) is screened too. It is NOT
    // guaranteed finite-positive: hash_rate_to_target (the open-time validator)
    // accepts 0.0/NaN/+inf, and the open handler doesn't screen — so a channel
    // can exist with a degenerate operating point. A plausible declaration
    // against a garbage reference must NOT ease (no trustworthy direction) — it
    // defers to the share loop. ----
    #[test]
    fn degenerate_reference_defers_not_eases() {
        let good = 50_000.0; // a plausible declaration
        // garbage-high reference: naive `declared < current` would read this as
        // "downward" and EASE — the screen must prevent that and DeferUp instead.
        assert_eq!(classify_hint(good, f32::INFINITY), HintAction::DeferUp);
        assert_eq!(classify_hint(good, f32::NAN), HintAction::DeferUp);
        // garbage-low / zero reference: also no trustworthy direction → defer.
        assert_eq!(classify_hint(good, 0.0), HintAction::DeferUp);
        assert_eq!(classify_hint(good, 1.0), HintAction::DeferUp); // sub-floor ref
        // and a garbage DECLARATION still loses to Reject regardless of reference
        // (declaration screened first).
        assert_eq!(classify_hint(f32::NAN, f32::NAN), HintAction::Reject);
    }

    #[test]
    fn reject_verdict_is_terminal_not_a_direction() {
        // A sub-floor nominal that is ALSO below current must be Reject, NOT
        // EaseDown — reject takes precedence over direction, so no ease fires on
        // a sentinel that happens to be "downward".
        assert_eq!(classify_hint(1.0, 100_000.0), HintAction::Reject);
        assert_ne!(classify_hint(1.0, 100_000.0), HintAction::EaseDown(1.0));
    }
}
