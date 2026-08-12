# Vardiff wall-time benchmark

Reproducible benchmark of every vardiff algorithm against every simulator
scenario, at **real wall-clock time** (`--speed 1`), producing a self-contained
HTML report.

Running at real time (rather than an accelerated sim clock) is deliberate: it
represents the true latencies a live pool sees — the SV2 noise handshake, TCP,
the idle-difficulty backstop, and the champion algorithm's fixed 60 s tick —
which an accelerated clock distorts (they'd occupy a larger fraction of virtual
time). Convergence and reaction-latency numbers are only trustworthy at 1×.

## Regenerate

One script drives everything — `benchmark/benchmark.sh` (build → run → report):

```sh
# Full pipeline: compile the sim, run 4 algorithms × all scenarios, rebuild the
# committed report end-to-end. With the in-process mock template provider (no
# bitcoind) startup is ~1 s/run; at SPEED=1 total time is bounded by scenario
# durations.
integration-tests/benchmark/benchmark.sh
```

```sh
# Just rebuild the report from the existing CSVs (fast, ~1 s — after editing the
# report tool, or to refresh from a run done elsewhere):
integration-tests/benchmark/benchmark.sh --report-only    # or REPORT_ONLY=1
```

```sh
# Collect CSVs only, no report (e.g. a scratch subset in a custom dir):
ALGOS="pid champion" SCENARIOS="convergence step-change" \
    integration-tests/benchmark/benchmark.sh --no-report /tmp/quickbench
# then build a report from them wherever you like:
cargo run --release --bin vardiff-bench-report -- /tmp/quickbench /tmp/quickbench/report.html
```

The run and report phases are decoupled (`--report-only` / `--no-report`), so you
can iterate on the report (metrics, charts) against an existing CSV set without a
full rerun.

### Options (flags + env vars)

Flags: `--report-only` (skip build+run), `--no-report` (build+run only),
`--help`. The first positional arg is `OUTDIR` (default
`benchmark-report/raw-csv`). Env overrides:

| var | default | meaning |
|-----|---------|---------|
| `ALGOS` | `classic pid qpid champion` | algorithms to run |
| `SCENARIOS` | all `scenarios/*.toml` | scenario names (space-separated) |
| `SPM` | `4` | vardiff setpoint (shares/minute) |
| `SPEED` | `1` | sim clock speed (1 = real time; the point of this bench) |
| `REPS` | `1` | repeats per (algo,scenario); the report pools a run's reps |
| `MAX_PARALLEL` | `12` | concurrent runs (light scenarios; heavy capped by `HEAVY_PARALLEL`, default 4) |
| `DURATION_MARGIN` | `30` | extra virtual seconds appended to each scenario |

**Reps (`REPS`).** A single run of a small-fleet scenario is noisy: the miners'
Poisson share arrivals vary run-to-run more than most real algorithm
differences, so single-run convergence/bandwidth numbers aren't trustworthy.
`REPS=N` runs each (algo, scenario) N times (CSVs suffixed `__repN`) and the
report **pools all reps of a run** — each rep's miners are namespaced
`<miner>@repN` and analyzed as one union, so never-converged sums across reps
and settled/operating-point/bandwidth percentiles average over the pool. Use
`REPS>=5` before drawing tuning conclusions. `REPS=1` writes the plain
`<algo>__<scenario>.csv` (unchanged, back-compatible).

## What's committed vs regenerable

- **Committed:** the HTML report (`vardiff-walltime-report.html`, self-contained
  — inline SVG charts, no external assets) and `analysis.json` (all computed
  metrics). Viewable without rerunning anything.
- **Gitignored:** `raw-csv/` (the ~55 MB of per-run CSVs) and its zip. Regenerate
  with the runner when you need the raw traces.

## Metrics (report sections)

- **Aggregate summary** per algorithm: settled accuracy p50/p99, peak overshoot,
  update churn, network bandwidth (average + peak), failure count.
- **Bandwidth** (total both directions): the on-wire SV2 byte cost of each
  frame (6-byte header + payload) is tallied at the miner — the same
  per-connection point that injects latency — separately for pool→miner
  (SetTarget, acks, jobs) and miner→pool (setup, shares). Reported as the
  fleet-wide **average** B/s over the run and the **peak** single-second rate
  (the worst burst). A chattier controller (more SetTarget churn) shows up here
  directly.
- **Convergence** after hashrate-step events: up (raise difficulty) / down
  (lower) median + max time to re-enter the ±20 % band, and a **"never
  converged"** count — a step that knocked the miner out of band and it never
  returned (the controller under-reacts to that direction).
- **Failures & outliers:** non-zero exits/timeouts, severe overshoot
  (>3× setpoint), stuck-off-target (p99 > 1.0), miners ending >2× setpoint.
- **Per-miner breakout:** every run's miners individually (settled p50/max,
  operating point, per-miner bandwidth, updates, never-converged), sorted
  worst-first with the worst N highlighted. The aggregate tables pool all
  miners into one distribution per run, which hides a bimodal fleet (some
  miners on setpoint, others stuck off-target); this section exposes that.
- **Per-scenario annotated time series:** mean expected share-rate for all four
  algorithms with scenario events marked, plus a per-scenario metric table.
- **Raw data:** embedded `analysis.json` (now including a `per_miner` array per
  run); per-run CSVs alongside.

The analysis/report tool is `bin/vardiff_bench_report/` — pure Rust, reusing the
real `Scenario` parser for exact event extraction and emitting dependency-free
inline SVG charts (no Python / matplotlib).
