//! Asserting integration tests for vardiff behavior.
//!
//! Every other vardiff artifact (the scenario corpus) only dumps CSVs for a
//! human to eyeball; these tests turn the key behavioral invariants into
//! pass/fail assertions so a controller regression fails CI. They drive the
//! simulated-miner engine against a real (share-validation-bypassing) sim pool
//! at an accelerated clock, then assert on converged difficulty / accepted
//! rates.
//!
//! These spin a real bitcoind + template provider (like the other integration
//! tests) and run virtual minutes compressed by the sim clock, so each is on
//! the order of tens of seconds of wall time. They are `#[ignore]` by default
//! so the fast unit suite stays fast; run them with:
//!   cargo test -p integration_tests_sv2 --test vardiff_behavior -- --ignored

use std::{sync::Arc, time::Duration};

use integration_tests_sv2::{
    start_tracing,
    vardiff_sim::{
        engine::{EngineConfig, SimEngine},
        pool::{start_sim_pool, AUTHORITY_PUBLIC_KEY},
        set_clock_speed, MinerConfig,
    },
};
use stratum_apps::key_utils::Secp256k1PublicKey;
use tokio::sync::Mutex;

/// Boot a sim pool + engine, spawn `miners`, run for `virtual_secs` of
/// simulated time at `speed`x, and return the final per-miner stats snapshot.
async fn run_fleet(
    algorithm: &str,
    spm: f32,
    speed: f64,
    virtual_secs: f64,
    miners: Vec<MinerConfig>,
) -> std::collections::HashMap<String, integration_tests_sv2::vardiff_sim::engine::MinerStats> {
    start_tracing();
    set_clock_speed(speed);

    let algorithm = match algorithm {
        "classic" => pool_sv2::config::VardiffAlgorithm::Classic,
        "pid" => pool_sv2::config::VardiffAlgorithm::Pid,
        "qpid" => pool_sv2::config::VardiffAlgorithm::QPid,
        "champion" => pool_sv2::config::VardiffAlgorithm::Champion,
        other => panic!("unknown algorithm {other}"),
    };
    let vardiff = pool_sv2::config::VardiffConfig {
        algorithm,
        ..Default::default()
    };
    let (pool, pool_addr, _tp) = start_sim_pool(spm, 10, vardiff).await;

    let authority_pubkey =
        Some(Secp256k1PublicKey::try_from(AUTHORITY_PUBLIC_KEY.to_string()).unwrap());
    let engine = Arc::new(Mutex::new(SimEngine::new(EngineConfig {
        pool_address: pool_addr,
        authority_pubkey,
    })));
    {
        let mut eng = engine.lock().await;
        for m in miners {
            eng.spawn_miner(m, None);
        }
    }

    // Drain events on a fixed wall cadence until enough virtual time elapses.
    let deadline_virtual = {
        let eng = engine.lock().await;
        eng.elapsed_secs() + virtual_secs
    };
    loop {
        {
            let mut eng = engine.lock().await;
            eng.drain_events();
            if eng.elapsed_secs() >= deadline_virtual {
                let stats = eng.stats.clone();
                drop(eng);
                let _ = pool.shutdown().await;
                set_clock_speed(1.0);
                return stats;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn miner(name: &str, hashrate: f64) -> MinerConfig {
    MinerConfig {
        name: name.to_string(),
        hashrate,
        ..Default::default()
    }
}

/// Core invariant: a clean fleet of varied sizes converges each channel's
/// accepted share rate toward the setpoint. Guards against a controller that
/// fails to converge at all.
#[tokio::test]
#[ignore = "spins bitcoind + TP; run with --ignored"]
async fn clean_fleet_converges_to_setpoint() {
    let spm = 6.0;
    let stats = run_fleet(
        "pid",
        spm,
        64.0,
        1200.0,
        vec![
            miner("small", 20e12),
            miner("medium", 150e12),
            miner("large", 600e12),
        ],
    )
    .await;
    for (name, s) in &stats {
        assert!(s.accepted > 0, "{name}: no accepted shares");
        let realized = s.realized_spm();
        // Wide band: convergence is stochastic, we only guard "in the right
        // ballpark of the setpoint", not precision.
        assert!(
            realized > 0.25 * spm as f64 && realized < 4.0 * spm as f64,
            "{name}: realized {realized:.1} spm not within 0.25x..4x of setpoint {spm}"
        );
    }
}

/// Invalid (rejected) shares must NOT feed vardiff: a faulty miner's rejects
/// accumulate while its accepted rate — the only thing vardiff sees — reflects
/// the valid share stream. Guards the whole bad-share plumbing.
#[tokio::test]
#[ignore = "spins bitcoind + TP; run with --ignored"]
async fn invalid_shares_are_rejected_and_ignored_by_vardiff() {
    let mut faulty = miner("faulty", 150e12);
    faulty.bad_share_fraction = 0.5;
    let stats = run_fleet(
        "pid",
        6.0,
        64.0,
        1200.0,
        vec![faulty, miner("clean", 150e12)],
    )
    .await;

    let faulty = &stats["faulty"];
    let clean = &stats["clean"];
    // The core invariant: the faulty miner actually produced rejects...
    assert!(
        faulty.rejected > 0,
        "faulty miner produced no rejects (bad-share path not exercised)"
    );
    // ...the clean miner produced none...
    assert_eq!(clean.rejected, 0, "clean miner should have zero rejects");
    // ...and submitted == accepted + rejected (accounting closes).
    assert_eq!(
        faulty.submitted,
        faulty.accepted + faulty.rejected,
        "faulty submitted != accepted + rejected"
    );
    // Vardiff only ever saw the accepted stream: the faulty channel still
    // converged to a sane, finite difficulty (not stranded at an extreme).
    assert!(
        faulty.difficulty > 0.0 && faulty.difficulty.is_finite(),
        "faulty difficulty not sane: {}",
        faulty.difficulty
    );
}

/// Duplicate/replayed shares are rejected as duplicates and likewise ignored
/// by vardiff. Guards the duplicate-share plumbing (a separate sentinel path).
#[tokio::test]
#[ignore = "spins bitcoind + TP; run with --ignored"]
async fn duplicate_shares_are_rejected_and_ignored_by_vardiff() {
    let mut dupe = miner("dupe", 150e12);
    dupe.duplicate_share_fraction = 0.5;
    let stats = run_fleet("pid", 6.0, 64.0, 1200.0, vec![dupe, miner("clean", 150e12)]).await;

    let dupe = &stats["dupe"];
    assert!(
        dupe.rejected > 0,
        "duplicate miner produced no rejects (dup path not exercised)"
    );
    assert_eq!(stats["clean"].rejected, 0);
    assert_eq!(
        dupe.submitted,
        dupe.accepted + dupe.rejected,
        "dupe submitted != accepted + rejected"
    );
    assert!(dupe.difficulty > 0.0 && dupe.difficulty.is_finite());
}

/// Delivery latency + jitter must not break share flow or crash the miner:
/// a laggy miner still gets shares accepted and converges to a finite
/// difficulty. Guards the latency plumbing.
#[tokio::test]
#[ignore = "spins bitcoind + TP; run with --ignored"]
async fn laggy_miner_still_converges() {
    let mut laggy = miner("laggy", 120e12);
    laggy.latency_ms = 2000;
    laggy.latency_jitter_ms = 1500;
    let stats = run_fleet("pid", 6.0, 32.0, 1200.0, vec![laggy, miner("local", 120e12)]).await;

    let laggy = &stats["laggy"];
    assert!(laggy.accepted > 0, "laggy miner got no shares accepted");
    assert!(
        laggy.difficulty > 0.0 && laggy.difficulty.is_finite(),
        "laggy difficulty not sane: {}",
        laggy.difficulty
    );
}
