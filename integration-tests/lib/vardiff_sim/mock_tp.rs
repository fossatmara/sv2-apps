//! Mock SV2 Template Provider for the vardiff simulator.
//!
//! The vardiff sim runs the pool with `ignore_share_validation = true`, so no
//! submitted share is ever checked against the block template and no solution
//! is propagated to a node. The chain is therefore irrelevant — the pool only
//! needs a template provider to (1) complete the SV2 handshake and (2) deliver
//! one `NewTemplate` + `SetNewPrevHash` so it opens its listener and can build
//! mining jobs. Difficulty adjustment is driven entirely by share arrival
//! rate, so a single static template suffices for the whole run.
//!
//! This mock replaces the real `bitcoind` + `sv2-tp` stack (a ~400 MB download,
//! IPC, and ~10 s startup per pool) with an in-process SV2 server that replays
//! real captured template frames. Pool startup drops to ~1 s, which is what
//! makes the benchmark practical to iterate on.
//!
//! The replayed frames were captured from a real bitcoind-backed run (see
//! `fixtures/`), so the pool accepts them byte-identically to a live TP — no
//! risk from hand-constructing a template's coinbase/merkle structure.

use std::net::SocketAddr;

use stratum_apps::stratum_core::parsers_sv2::{AnyMessage, TemplateDistribution};

use crate::utils::create_downstream;

/// Captured real TP frames: `[msg_type_byte][sv2 payload]`.
const NEW_TEMPLATE_BIN: &[u8] = include_bytes!("fixtures/new_template.bin");
const SET_NEW_PREV_HASH_BIN: &[u8] = include_bytes!("fixtures/set_new_prev_hash.bin");

/// Decodes a captured `[msg_type][payload]` blob into a `TemplateDistribution`
/// message (owned/static so it can be re-serialized to a frame), the same way
/// the pool's TP receiver decodes an incoming frame.
fn decode(bin: &[u8]) -> TemplateDistribution<'static> {
    let msg_type = bin[0];
    let mut payload = bin[1..].to_vec();
    TemplateDistribution::try_from((msg_type, payload.as_mut_slice()))
        .expect("captured TP frame decodes")
        .into_static()
}

/// Spawns the mock TP: binds `addr`, accepts one pool connection, completes the
/// Noise handshake (responder, using the shared test authority key the pool
/// trusts), answers `SetupConnection` with `SetupConnectionSuccess`, then
/// replays the captured `NewTemplate` + `SetNewPrevHash`. Any later requests
/// (e.g. `CoinbaseOutputConstraints`, `RequestTransactionData`) are drained and
/// ignored — the static template never changes.
///
/// Returns a task handle; drop it (or let the process exit) to stop the mock.
pub fn spawn(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("mock-tp: bind {addr} failed: {e}");
                return;
            }
        };
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            // Noise responder handshake (reuses the pool-trusted test keypair).
            let Some((from_pool, to_pool)) = create_downstream(stream).await else {
                eprintln!("mock-tp: noise handshake failed");
                continue;
            };

            // Handshake: the pool sends SetupConnection first; reply Success.
            // We don't gate on it strictly — the first frame is SetupConnection.
            if let Ok(mut frame) = from_pool.recv().await {
                use stratum_apps::stratum_core::{
                    common_messages_sv2::SetupConnectionSuccess,
                    framing_sv2::framing::Sv2Frame,
                    parsers_sv2::CommonMessages,
                };
                let _ = &mut frame; // (parsing not required; the pool only needs Success)
                let success = SetupConnectionSuccess {
                    used_version: 2,
                    flags: 0,
                };
                let msg = AnyMessage::Common(CommonMessages::SetupConnectionSuccess(success));
                if let Ok(f) = TryInto::<Sv2Frame<AnyMessage<'static>, _>>::try_into(msg) {
                    let _ = to_pool
                        .send(stratum_apps::stratum_core::codec_sv2::StandardEitherFrame::Sv2(f))
                        .await;
                }
            }

            // Deliver the template so the pool opens its listener. Order
            // matters: NewTemplate first (it registers the template_id and
            // future job), then SetNewPrevHash (which looks that job up) —
            // the same order the real TP sends them.
            for bin in [NEW_TEMPLATE_BIN, SET_NEW_PREV_HASH_BIN] {
                let msg = AnyMessage::TemplateDistribution(decode(bin));
                if let Ok(f) = TryInto::<
                    stratum_apps::stratum_core::framing_sv2::framing::Sv2Frame<AnyMessage<'static>, _>,
                >::try_into(msg)
                {
                    let _ = to_pool
                        .send(stratum_apps::stratum_core::codec_sv2::StandardEitherFrame::Sv2(f))
                        .await;
                }
            }

            // Drain any further pool requests (coinbase constraints, tx data)
            // and ignore them; the static template is all vardiff needs.
            while from_pool.recv().await.is_ok() {}
        }
    })
}
