#!/usr/bin/env bash
#
# Regenerate the vardiff wall-time benchmark: every algorithm against every
# scenario, at real time (--speed 1), producing one CSV per run.
#
# Usage:
#   integration-tests/benchmark/run-benchmark.sh [OUTDIR]
#
# Env overrides:
#   ALGOS="classic pid qpid champion"   algorithms to run
#   SCENARIOS="convergence step-change"  scenario names (default: all)
#   SPM=4              vardiff setpoint (shares/minute)
#   SPEED=1            sim clock speed (1 = real wall time, the point of this bench)
#   MAX_PARALLEL=6     concurrent runs
#   DURATION_MARGIN=30 extra virtual seconds appended to each scenario's duration
#
# Each run is an independent process with its own pool + bitcoind + template
# provider (port-keyed datadirs, so parallel runs don't collide). Output:
#   OUTDIR/<algo>__<scenario>.csv   one row per miner per virtual second
#   OUTDIR/<algo>__<scenario>.log   stdout/stderr
#   OUTDIR/_progress.log            "<tag> exit=<rc> wall=<s>s dur=<s>s" per run
#
# After this completes, build the report with:
#   cargo run --release --bin vardiff-bench-report -- OUTDIR
set -u

# Resolve repo paths relative to this script, so it works from any CWD.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"          # integration-tests/
cd "$IT_DIR"

OUTDIR="${1:-$IT_DIR/benchmark-report/raw-csv}"
BIN="$IT_DIR/target/release/vardiff-sim"
SPM="${SPM:-4}"
SPEED="${SPEED:-1}"
MAX_PARALLEL="${MAX_PARALLEL:-6}"
DURATION_MARGIN="${DURATION_MARGIN:-30}"
ALGOS="${ALGOS:-classic pid qpid champion}"

if [ ! -x "$BIN" ]; then
  echo "building release vardiff-sim (not found at $BIN)..." >&2
  cargo build --release --bin vardiff-sim || { echo "build failed" >&2; exit 1; }
fi

if [ -z "${SCENARIOS:-}" ]; then
  SCENARIOS="$(ls scenarios/*.toml | xargs -n1 basename | sed 's/\.toml//')"
fi

mkdir -p "$OUTDIR"
: > "$OUTDIR/_progress.log"
echo "benchmark: algos=[$ALGOS] speed=$SPEED spm=$SPM parallel=$MAX_PARALLEL out=$OUTDIR" | tee -a "$OUTDIR/_progress.log"

run_one() {
  local algo="$1" scen="$2"
  local dur
  dur="$(grep -m1 duration_secs "scenarios/${scen}.toml" | grep -oE '[0-9]+')"
  dur="${dur:-600}"
  local tag="${algo}__${scen}" t0 t1 rc
  t0="$(date +%s)"
  # timeout margin generous over duration so real-time runs finish cleanly.
  timeout "$((dur + 180))" "$BIN" --spawn-pool --algorithm "$algo" \
    --shares-per-minute "$SPM" --speed "$SPEED" --duration "$((dur + DURATION_MARGIN))" \
    --scenario "scenarios/${scen}.toml" --csv "$OUTDIR/${tag}.csv" \
    >"$OUTDIR/${tag}.log" 2>&1
  rc=$?
  t1="$(date +%s)"
  echo "${tag} exit=${rc} wall=$((t1 - t0))s dur=${dur}s" | tee -a "$OUTDIR/_progress.log"
}

jobs_running() { jobs -rp | wc -l; }

# Heavy scenarios (many miners) run with reduced concurrency so their
# connection storms don't starve a full parallel batch.
is_heavy() { [ "$(grep -c '^\[\[miners\]\]' "scenarios/$1.toml")" -ge 20 ]; }

# Pass 1: heavy scenarios, low concurrency.
for scen in $SCENARIOS; do
  is_heavy "$scen" || continue
  for algo in $ALGOS; do
    while [ "$(jobs_running)" -ge 2 ]; do sleep 2; done
    run_one "$algo" "$scen" & sleep 8
  done
done
wait

# Pass 2: the rest, full concurrency.
for algo in $ALGOS; do
  for scen in $SCENARIOS; do
    is_heavy "$scen" && continue
    while [ "$(jobs_running)" -ge "$MAX_PARALLEL" ]; do sleep 2; done
    run_one "$algo" "$scen" & sleep 4
  done
done
wait

n_ok="$(grep -c 'exit=0 ' "$OUTDIR/_progress.log" || true)"
n_all="$(grep -c 'exit=' "$OUTDIR/_progress.log" || true)"
echo "ALL DONE $(date -u +%H:%M:%S)  ($n_ok/$n_all runs exit=0)" | tee -a "$OUTDIR/_progress.log"
echo "CSVs: $(ls "$OUTDIR"/*.csv 2>/dev/null | wc -l) in $OUTDIR"
echo "next: cargo run --release --bin vardiff-bench-report -- $OUTDIR"
