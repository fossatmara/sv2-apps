//! Convenience bootstrap: a local template provider + pool configured for
//! simulation (`ignore_share_validation = true`).

use std::{convert::TryFrom, net::SocketAddr, time::Duration};

use pool_sv2::PoolSv2;
use stratum_apps::{
    config_helpers::CoinbaseRewardScript,
    key_utils::{Secp256k1PublicKey, Secp256k1SecretKey},
};

use crate::{
    start_template_provider, sv2_tp_config,
    template_provider::{DifficultyLevel, TemplateProvider},
    utils::get_available_address,
    POOL_COINBASE_REWARD_ADDRESS,
};

/// Test authority keypair (same one the integration tests use).
pub const AUTHORITY_PUBLIC_KEY: &str = "9auqWEzQDVyd2oe1JVGFLMLHZtCo2FFqZwtKA5gd9xbuEu7PH72";
const AUTHORITY_SECRET_KEY: &str = "mkDLTBBRxdBv998612qipDYoTK3YUrqLe8uWw7gu3iXbSrn2n";

/// Starts a regtest template provider plus a pool that accepts every share
/// without validation. Returns the pool handle, its listen address, and the
/// template provider handle (keep both alive for the duration of the run).
///
/// `vardiff_interval_secs` controls how often the pool re-evaluates channel
/// difficulty. Note the classic algorithm ignores windows of 15s or less, so
/// values below ~16 have no additional effect.
pub async fn start_sim_pool(
    shares_per_minute: f32,
    vardiff_interval_secs: u64,
) -> (PoolSv2, SocketAddr, TemplateProvider) {
    let (template_provider, tp_address) = start_template_provider(None, DifficultyLevel::Low);

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

    let pool = PoolSv2::new(config);
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        let _ = pool_clone.start().await;
    });
    // The pool opens its listener only after the first template arrives.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        match tokio::net::TcpStream::connect(listen_address).await {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(e) => panic!("pool never opened its listener on {listen_address}: {e}"),
        }
    }
    (pool, listen_address, template_provider)
}
