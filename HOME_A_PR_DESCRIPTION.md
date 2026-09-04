# Guard `UpdateChannel`/`OpenChannel` against the over-difficulty spiral (eager-ease / reluctant-tighten)

> **This is the LAYER 2 document — the pool-side guard, in `sv2-apps`.** It is one
> of three layers; do not mistake it for the upstream `stratum` contribution.
> - **Layer 1 (stratum, proposable now):** the robustness fix to
>   `channels_sv2::hash_rate_to_target` — reject non-finite input at the source.
>   General, benefits every consumer, needs no live data. See
>   `stratum/LAYER1_PR_DESCRIPTION.md` (committed on branch
>   `fix/hash-rate-to-target-reject-non-finite`).
> - **Layer 2 (this doc):** the pool-side eager-ease/reluctant-tighten guard.
>   Built, tested, deployable. Its decision logic is unit-tested; its live
>   downward-ease behavior is **evidence-gathering** — it cannot be validated until
>   the topology produces native-SV2 per-device `UpdateChannel` traffic (the pool
>   currently sees one aggregate channel).
> - **Layer 3 (stratum, gated):** lifting the eager-ease/reluctant-tighten
>   *behavioral asymmetry* into the reference implementation. The most valuable
>   layer and the most gated — it wants the production evidence Layer 2 gathers,
>   which is itself blocked on the topology. Parked on that gate, not on packaging.
>
> The robustness fix (Layer 1) is kept deliberately clean of the behavioral policy
> (Layer 3): the moment a PR mixes them, it inherits Layer 3's evidence gate. This
> guard is the workaround that protects *this pool* now and gathers the evidence
> the upstream behavioral change will eventually rest on.

## What this is

A safety fix for the pool's mining-channel handler. Today `handle_update_channel`
calls `update_channel` **unconditionally**, so the pool tightens a channel's
operating point on the miner's *unverified* `nominal_hash_rate` on every upward
revision. That is the unguarded-upward injection that opens the over-difficulty
spiral: a single overstated hint raises the difficulty bar, the share rate falls,
and on a struggling miner the controller can chase it down toward disconnect.

This PR replaces the unconditional retarget with the **eager-ease /
reluctant-tighten asymmetry** at both channel entry points, and screens the
declared nominal for plausibility before acting on it.

It is a **bug fix first, a feature second.** The feature is "the pool honors a
downward hint"; the fix is "the pool no longer tightens on the miner's say-so."

## The asymmetry

A revision is classified (`classify_hint`) into three actions (`HintAction`):

| Declared nominal | Action | Why |
|---|---|---|
| plausible, **downward** | **`EaseDown`** — retarget to the declared nominal, clamped to `min(miner_max, pool_floor)` | easing is the safe, self-healing direction: a false downward hint costs only a bounded, self-correcting share burst |
| plausible, **upward** | **`DeferUp`** — do *not* tighten; leave tightening to the share-driven vardiff loop | tightening on an unbacked claim is the spiral; the share loop owns it on corroborated evidence |
| implausible (sentinel/garbage) | **`Reject`** — drop the message | a `nominal=1` sentinel or non-finite value is not actionable |

The asymmetry is **forced by a missing protocol field**, not a stylistic choice:
`UpdateChannel` carries no device count, so the pool cannot distinguish a
legitimate aggregate-attach upward revision from an unbacked say-so. Given that
ambiguity, easing on a false hint is cheap (a bounded share burst that
self-corrects) and tightening on one is the spiral — so the safe default is to
ease eagerly and defer tightening to evidence.

## Plausibility floor (both entry points)

A shared check, `is_plausible_nominal` — *finite AND ≥ `MIN_PLAUSIBLE_NOMINAL_HS`
(1000 H/s)* — screens the declared nominal at **both** `UpdateChannel`
(`classify_hint`) and `OpenChannel`, so they reject identically. This catches:

- the `nominal=1` sentinel seen in the field, and
- the non-finite values (`0.0`/`NaN`/`+inf`) that `channels_sv2`'s
  `hash_rate_to_target` silently accepts — it rejects only negative-hashrate /
  zero-spm, so those cast to a "valid" `u128` and yield a garbage target.

**Both operands are screened.** The direction comparison `declared <
current_nominal` is only as trustworthy as both sides, and `current_nominal` is
*not* guaranteed finite-positive: it is the fire-path register, seeded at open
from the declared open nominal through the same permissive converter, and the
open handler did not previously screen it. So a channel can legitimately exist
with a degenerate reference; a plausible declaration against a degenerate
reference **defers** (the safe leg) rather than easing on a garbage direction
comparison.

**Cold-start (`OpenChannel`):** an implausible *open* nominal is treated as a
*reporting* fault, not a *hashing* fault — so the channel **opens at a
conservative default** (`COLD_START_DEFAULT_NOMINAL_HS` = the floor, routed
through the same `hash_rate_to_target` the real path uses) rather than being
rejected. Rejecting the open would drop a hashing-capable miner over a bad sensor
reading. Polarity is deliberate: the floor nominal yields the *easiest* plausible
open target → the miner over-produces shares → the controller floods (the
self-healing direction) → the EWMA tightens up fast. Opening *hard* would starve
the controller at birth (the spiral direction).

## What's tested, and what is not (scope)

The classifier and screen are **pure functions**, and their decision logic is
pinned by **12 unit tests**: `is_plausible_nominal` (finite-and-floor), the
cold-start default substitution, all three `classify_hint` branches, the
`1000.0` boundary (accepted, `<` not `<=`), `NaN`/`±inf` rejection (the
silent-pass guard — the screen-order is load-bearing and tested), the ease clamp
by explicit target value (catches a `.min`→`.max` inversion), degenerate-reference
defers, and reject-is-terminal.

**Live validation of the downward-ease path is pending and is not claimed here.**
The upward-defer and downward-ease *logic* is unit-tested; it has **not** been
exercised against live native-SV2 per-device `UpdateChannel` traffic, because the
current sv1 → translator → pool topology does not yet produce it (the pool sees
one aggregate channel, not per-device revisions). That validation will follow
once the topology produces native-SV2 hints; this PR ships the guard with its
decision logic unit-tested and its live behavior pending that traffic.

## Scope / footprint

- **One file:** `pool-apps/pool/src/lib/channel_manager/mining_message_handler.rs`.
- **No `channels_sv2` / `Vardiff` trait change.** The guard sits *upstream* of the
  trait and writes the channel's existing operating-point register via the
  existing `update_channel` — it is policy in front of the handler, not a control-loop change.
- **No `Cargo.lock` change.**

## Known gaps — all scoped, reversible, one missing operand

Three deviations all trace to a single absent config operand — **a pool difficulty
band/ceiling** — and all close at once when it is configured:

1. The pool-floor clamp on the downward ease ships **non-biting**
   (`POOL_FLOOR_HASHRATE = 1.0`) so it cannot perturb the reference-baseline
   deployment; real bounding awaits an operator-set band.
2. The `Reject` branch drops the message's `max_target` shrink — a shrink moves
   difficulty *up* and a hostile-tight shrink is a difficulty-slam DoS with no
   ceiling to screen it. Reverts to spec-literal (honor a screened shrink) once a
   ceiling exists.
3. The open path honors a tight `max_target` as the miner's self-inflicted bound —
   again unscreenable without a ceiling. (A `Target` is a 256-bit int, so there is
   no *non-finite* `max_target`; the type closes that case. The open gap is
   *hostile-tight values*, which need the ceiling.)

Each is recorded in-code with its reversibility condition.

## Related / follow-up (not in this PR)

The root cause is upstream and library-side: `channels_sv2`'s `hash_rate_to_target`
accepts `0.0`/`NaN`/`+inf`. This pool screen is the workaround *and* the spec for
the eventual library fix (the validator should reject non-finite at the source, so
every SRI consumer is covered). That fix, together with a `nominal_hash_rate`
field-split + `apply_hint` API, is a separate later proposal — gated on live data
from this guard.
