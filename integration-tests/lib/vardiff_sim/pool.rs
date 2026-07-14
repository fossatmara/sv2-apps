//! Convenience bootstrap: a mock SV2 template provider + pool configured for
//! simulation (`ignore_share_validation = true`).
//!
//! The vardiff sim never validates shares or propagates solutions, so it does
//! not need a real chain. Instead of a `bitcoind` + `sv2-tp` stack (a ~400 MB
//! download, IPC, and ~10 s startup per pool) this uses an in-process
//! [`mock_tp`] that replays captured real template frames — pool startup drops
//! to ~1 s, which is what makes the benchmark practical to iterate on.

use std::{convert::TryFrom, net::SocketAddr, time::Duration};

use pool_sv2::PoolSv2;
use stratum_apps::{
    config_helpers::CoinbaseRewardScript,
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
};

use crate::{sv2_tp_config, utils::get_available_address, POOL_COINBASE_REWARD_ADDRESS};

use super::mock_tp;

/// Test authority keypair (same one the integration tests use).
pub const AUTHORITY_PUBLIC_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
const AUTHORITY_SECRET_KEY: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

/// Handle to the mock template provider task; hold it for the run's lifetime.
/// Dropping it aborts the mock (harmless — the pool already has its template).
pub struct MockTp(tokio::task::JoinHandle<()>);
impl Drop for MockTp {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Starts a mock template provider plus a pool that accepts every share without
/// validation. Returns the pool handle, its listen address, and the mock-TP
/// handle (keep all alive for the duration of the run).
///
/// `vardiff_interval_secs` controls how often the pool re-evaluates channel
/// difficulty. Note the classic algorithm ignores windows of 15s or less
/// (so values below ~16 have no effect there); the pid algorithm accepts
/// arbitrarily short intervals and gates on statistical significance
/// instead.
pub async fn start_sim_pool(
    shares_per_minute: f32,
    vardiff_interval_secs: u64,
    vardiff: pool_sv2::config::VardiffConfig,
) -> (PoolSv2, SocketAddr, MockTp) {
    // Spawn the in-process mock TP on its own port; the pool connects to it as
    // an ordinary SV2 template provider.
    let tp_address = get_available_address();
    let mock = mock_tp::spawn(tp_address).await;

    let listen_address = get_available_address();
    let authority_public_key = Secp256k1PublicKey::try_from(AUTHORITY_PUBLIC_KEY.to_string())
        .expect("static key is valid");
    let authority_secret_key = Secp256k1SecretKey::try_from(AUTHORITY_SECRET_KEY.to_string())
        .expect("static key is valid");
    let coinbase_reward_script = CoinbaseRewardScript::from_descriptor(&format!(
        "addr({POOL_COINBASE_REWARD_ADDRESS})"
    ))
    .expect("static descriptor is valid");

    let connection_config = pool_sv2::config::ConnectionConfig::new(
        listen_address,
        3600,
        "Vardiff Sim Pool".to_string(),
    );
    let authority_config =
        pool_sv2::config::AuthorityConfig::new(authority_public_key, authority_secret_key);
    let mut config = pool_sv2::config::PoolConfig::new(
        connection_config,
        sv2_tp_config(tp_address),
        authority_config,
        coinbase_reward_script,
        shares_per_minute,
        1, // acknowledge every share so the simulator sees each acceptance
        1,
        vec![],
        vec![],
        None,
        None,
        None, // no JDS
    );
    config.set_ignore_share_validation(true);
    config.set_vardiff_interval_secs(vardiff_interval_secs);
    config.set_vardiff(vardiff);

    let pool = PoolSv2::new(config);
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        let _ = pool_clone.start().await;
    });
    // The pool opens its listener only after the first template arrives (from
    // the mock). With no bitcoind/IPC this is ~1 s, so a short timeout is fine.
    if !wait_for_pool(listen_address, Duration::from_secs(30)).await {
        panic!("pool never opened its listener on {listen_address}");
    }
    (pool, listen_address, MockTp(mock))
}

/// Waits until a pool accepts TCP connections on `address`. Returns false if
/// the timeout expires first.
pub async fn wait_for_pool(address: SocketAddr, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match tokio::net::TcpStream::connect(address).await {
            Ok(_) => return true,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(_) => return false,
        }
    }
}
