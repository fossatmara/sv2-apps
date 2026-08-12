use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use stratum_apps::{
    key_utils::Secp256k1PublicKey,
    stratum_core::{
        binary_sv2::U256,
        bitcoin::{
            block::{Header, Version},
            hashes::Hash,
            CompactTarget, Target, TxMerkleNode,
        },
        channels_sv2::{
            merkle_root::merkle_root_from_path,
            target::{bytes_to_hex, u256_to_block_hash},
        },
        sv1_api::{
            client_to_server::{self, Submit},
            json_rpc,
            server_to_client::Notify,
            utils::HexU32Be,
            Message,
        },
    },
    utils::types::{ChannelId, DownstreamId},
};

use tracing::{debug, warn};

use crate::error::TproxyErrorKind;

/// Channel ID used to broadcast messages to all downstreams in aggregated mode.
/// This sentinel value distinguishes broadcast from a legitimate channel 0.
pub const AGGREGATED_CHANNEL_ID: ChannelId = u32::MAX;

/// Validates an SV1 share against the target difficulty and job parameters.
///
/// This function performs complete share validation by:
/// 1. Finding the corresponding job from the valid jobs storage
/// 2. Constructing the full extranonce from extranonce1 and extranonce2
/// 3. Calculating the merkle root from the coinbase transaction and merkle path
/// 4. Building the block header with the share's nonce and timestamp
/// 5. Hashing the header and comparing against the target difficulty
///
/// # Arguments
/// * `share` - The SV1 submit message containing the share data
/// * `target` - The target difficulty for this share
/// * `extranonce1` - The first part of the extranonce (from server)
/// * `version_rolling_mask` - Optional mask for version rolling
/// * `sv1_server_data` - Reference to shared SV1 server data for accessing valid jobs
/// * `channel_id` - Channel ID for job lookup
///
/// # Returns
/// * `Ok(true)` if the share is valid and meets the target
/// * `Ok(false)` if the share is valid but doesn't meet the target
/// * `Err(TproxyError)` if validation fails due to missing job or invalid data
pub fn validate_sv1_share(
    share: &client_to_server::Submit<'static>,
    target: Target,
    extranonce1: Vec<u8>,
    version_rolling_mask: Option<HexU32Be>,
    job: Notify<'static>,
) -> Result<bool, TproxyErrorKind> {
    let mut full_extranonce = vec![];
    full_extranonce.extend_from_slice(extranonce1.as_slice());
    full_extranonce.extend_from_slice(share.extra_nonce2.0.as_ref());

    let share_version = share
        .version_bits
        .clone()
        .map(|vb| vb.0)
        .unwrap_or(job.version.0);
    let mask = version_rolling_mask.unwrap_or(HexU32Be(0x1FFFE000_u32)).0;
    let version = (job.version.0 & !mask) | (share_version & mask);

    let prev_hash: U256<'static> = Vec::<u8>::from(job.prev_hash.clone())
        .try_into()
        .map_err(TproxyErrorKind::BinarySv2)?;

    // calculate the merkle root from:
    // - job coinbase_tx_prefix
    // - full extranonce
    // - job coinbase_tx_suffix
    // - job merkle_path
    let merkle_root: [u8; 32] = merkle_root_from_path(
        job.coin_base1.as_ref(),
        job.coin_base2.as_ref(),
        full_extranonce.as_ref(),
        job.merkle_branch.as_ref(),
    )
    .ok_or(TproxyErrorKind::InvalidMerkleRoot)?
    .try_into()
    .map_err(|_| TproxyErrorKind::InvalidMerkleRoot)?;

    // create the header for validation
    let header = Header {
        version: Version::from_consensus(version as i32),
        prev_blockhash: u256_to_block_hash(prev_hash),
        merkle_root: TxMerkleNode::from_byte_array(merkle_root),
        time: share.time.0,
        bits: CompactTarget::from_consensus(job.bits.0),
        nonce: share.nonce.0,
    };

    // convert the header hash to a target type for easy comparison
    let hash = header.block_hash();
    let raw_hash: [u8; 32] = *hash.to_raw_hash().as_ref();
    let hash_as_target = Target::from_le_bytes(raw_hash);

    // print hash_as_target and self.target as human readable hex
    let hash_bytes = hash_as_target.to_be_bytes();
    let target_bytes = target.to_be_bytes();

    debug!(
        "share validation \nshare:\t\t{}\ndownstream target:\t{}\n",
        bytes_to_hex(&hash_bytes),
        bytes_to_hex(&target_bytes),
    );
    // check if the share hash meets the downstream target
    if hash_as_target < target {
        /*if self.share_accounting.is_share_seen(hash.to_raw_hash()) {
            return Err(ShareValidationError::DuplicateShare);
        }*/

        return Ok(true);
    }

    Ok(false)
}

/// Tracks the state of the single upstream extended channel in aggregated mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregatedState {
    /// No upstream channel has been opened yet.
    NoChannel = 0,
    /// An `OpenExtendedMiningChannel` has been sent and we are waiting for the success response.
    Pending = 1,
    /// The upstream channel is open and ready to accept new downstream connections directly.
    Connected = 2,
}

/// Atomic wrapper around `UpstreamState` for lock-free state transitions.
#[derive(Clone, Debug)]
pub struct AtomicAggregatedState {
    inner: Arc<AtomicU8>,
}

impl AtomicAggregatedState {
    pub fn new(state: AggregatedState) -> Self {
        Self {
            inner: Arc::new(AtomicU8::new(state as u8)),
        }
    }

    pub fn get(&self) -> AggregatedState {
        match self.inner.load(Ordering::SeqCst) {
            0 => AggregatedState::NoChannel,
            1 => AggregatedState::Pending,
            2 => AggregatedState::Connected,
            v => panic!("Invalid UpstreamState value: {v}"),
        }
    }

    pub fn set(&self, state: AggregatedState) {
        self.inner.store(state as u8, Ordering::SeqCst);
    }
}

#[derive(Debug)]
pub struct UpstreamEntry {
    /// Upstream host — can be an IP address or a hostname (resolved at connection time).
    pub host: String,
    pub port: u16,
    pub authority_pubkey: Secp256k1PublicKey,
    pub tried_or_flagged: bool,
    pub user_identity: String,
}

/// Defines the operational mode for Translator Proxy.
///
/// It can operate in two different modes that affect how Sv1
/// downstream connections are mapped to the upstream Sv2 channels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TproxyMode {
    /// All Sv1 downstream connections share a single extended Sv2 channel.
    /// This mode uses extranonce_prefix allocation to distinguish between
    /// different downstream miners while presenting them as a single entity
    /// to the upstream server. This is more efficient for pools with many
    /// miners.
    Aggregated,
    /// Each Sv1 downstream connection gets its own dedicated extended Sv2 channel.
    /// This mode provides complete isolation between downstream connections
    /// but may be less efficient for large numbers of miners.
    NonAggregated,
}

impl From<bool> for TproxyMode {
    fn from(value: bool) -> Self {
        if value {
            return TproxyMode::Aggregated;
        }
        TproxyMode::NonAggregated
    }
}

impl TproxyMode {
    pub(crate) fn is_aggregated(self) -> bool {
        TproxyMode::Aggregated == self
    }

    pub(crate) fn is_non_aggregated(self) -> bool {
        TproxyMode::NonAggregated == self
    }
}

/// Messages sent from downstream handling logic to the SV1 server.
///
/// This enum defines the types of messages that downstream connections can send
/// to the central SV1 server for processing and forwarding to upstream.
#[derive(Debug)]
pub enum DownstreamMessages {
    /// Represents a submitted share from a downstream miner,
    /// wrapped with the relevant channel ID.
    SubmitShares(SubmitShareWithChannelId),
    /// Request to open an extended mining channel for a downstream that just sent its first
    /// message.
    OpenChannel(DownstreamId), // downstream_id
}

/// A wrapper around a `mining.submit` message with additional channel information.
///
/// This struct contains all the necessary information to process a share submission
/// from an SV1 miner, including the share data itself and metadata needed for
/// proper routing and validation.
#[derive(Debug, Clone)]
pub struct SubmitShareWithChannelId {
    /// The SV2 channel ID this share belongs to
    pub channel_id: ChannelId,
    /// The downstream connection ID that submitted this share
    pub downstream_id: DownstreamId,
    /// The actual SV1 share submission data
    pub share: Submit<'static>,
    /// The complete extranonce used for this share
    pub extranonce: Vec<u8>,
    /// The length of the extranonce2 field
    pub extranonce2_len: usize,
    /// Optional version rolling mask for the share
    pub version_rolling_mask: Option<HexU32Be>,
    /// The version field from the job, used for validation
    pub job_version: Option<u32>,
}

/// Delimiter used to separate original job ID from keepalive mutation counter.
/// Format: `{original_job_id}#{counter}`
pub(crate) const KEEPALIVE_JOB_ID_DELIMITER: char = '#';

/// Check if Sv1 message is mining.authorize
pub(crate) fn is_mining_authorize(msg: &Message) -> bool {
    if let json_rpc::Message::StandardRequest(r) = &msg {
        r.method == "mining.authorize"
    } else {
        false
    }
}

/// Truncates a string to [`MAX_USER_IDENTITY_BYTES`], respecting UTF-8 character boundaries.
///
/// If the input string exceeds the limit, it is truncated at the last valid UTF-8 character
/// boundary before or at [`MAX_USER_IDENTITY_BYTES`] and a warning is logged.
pub(crate) fn tlv_compatible_username(s: &str) -> &str {
    const MAX_USER_IDENTITY_BYTES: usize = 32;
    let len = s.len();

    if len <= MAX_USER_IDENTITY_BYTES {
        return s;
    }
    // Find the last valid UTF-8 char boundary at or before MAX_USER_IDENTITY_BYTES
    let mut end = MAX_USER_IDENTITY_BYTES;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    let truncated = &s[..end];
    warn!(
        "Username '{}' exceeds {} bytes ({} bytes), truncating to '{}'. \
         Consider using a shorter username for full visibility on the pool dashboard.",
        s, MAX_USER_IDENTITY_BYTES, len, truncated
    );
    truncated
}
