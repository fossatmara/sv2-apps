#!/usr/bin/env bash
#
# Vardiff wall-time benchmark — one script, three phases:
#   build  -> compile release vardiff-sim
#   run    -> every algorithm x every scenario, one CSV per run
#   report -> build the self-contained HTML report + analysis.json
#
# (Unifies the former run-benchmark.sh + regenerate.sh into one entry point.)
#
# Usage:
#   integration-tests/benchmark/benchmark.sh [OUTDIR]
#     Full pipeline: build -> run -> report. OUTDIR defaults to the committed
#     benchmark-report/raw-csv (so the committed HTML/analysis.json refresh).
#
# Modes (set by either the flag or the env var; the two modes are mutually
# exclusive — selecting both, by any mix of flag/env, is an error):
#   --report-only   | REPORT_ONLY=1   skip build+run, rebuild the report from
#                                      the CSVs already in OUTDIR (fast, ~1s;
#                                      use after editing the report tool)
#   --no-report     | NO_REPORT=1      build+run only, no report (matches the
#                                      old run-benchmark.sh; for scratch CSV sets
#                                      in a custom OUTDIR)
#
# Env overrides (forwarded to the run phase):
#   ALGOS="classic pid qpid champion"   algorithms to run
#   SCENARIOS="convergence step-change" scenario names (default: all scenarios/*.toml)
#   SPM=4              vardiff setpoint (shares/minute)
#   SPEED=1            sim clock speed (1 = real wall time, the point of this bench)
#   MAX_PARALLEL=6     concurrent runs
#   DURATION_MARGIN=30 extra virtual seconds appended to each scenario's duration
#
# Each run is an independent process with its own pool + in-process mock
# template provider (port-keyed, so parallel runs don't collide). Output:
#   OUTDIR/<algo>__<scenario>.csv   one row per miner per virtual second
#   OUTDIR/<algo>__<scenario>.log   stdout/stderr
#   OUTDIR/_progress.log            "<tag> exit=<rc> wall=<s>s dur=<s>s" per run
#
# Committed report outputs (full-pipeline / --report-only, default OUTDIR):
#   benchmark-report/vardiff-walltime-report.html
#   benchmark-report/analysis.json
# raw-csv/ is gitignored (regenerable); commit the HTML + analysis.json.
set -u

# ---- resolve paths + parse mode -------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)" # integration-tests/
cd "$IT_DIR"

REPORT_ONLY="${REPORT_ONLY:-0}"
NO_REPORT="${NO_REPORT:-0}"
POSITIONAL=()
for arg in "$@"; do
  case "$arg" in
    --report-only) REPORT_ONLY=1 ;;
    --no-report) NO_REPORT=1 ;;
    -h | --help)
      sed -n '2,/^set -u/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//; /^set -u/d'
      exit 0
      ;;
    --*)
      echo "unknown flag: $arg" >&2
      exit 2
      ;;
    *) POSITIONAL+=("$arg") ;;
  esac
done

DEFAULT_OUTDIR="$IT_DIR/benchmark-report/raw-csv"
OUTDIR="${POSITIONAL[0]:-$DEFAULT_OUTDIR}"
# The report lands with the *committed* HTML only when writing to the default
# CSV dir; a custom OUTDIR (a scratch run) gets its report alongside it, so a
# partial/experimental run never clobbers the committed 104-run report.
if [ "$OUTDIR" = "$DEFAULT_OUTDIR" ]; then
  REPORT="$IT_DIR/benchmark-report/vardiff-walltime-report.html"
else
  REPORT="$OUTDIR/report.html"
fi
BIN="$IT_DIR/target/release/vardiff-sim"
SPM="${SPM:-4}"
SPEED="${SPEED:-1}"
MAX_PARALLEL="${MAX_PARALLEL:-6}"
DURATION_MARGIN="${DURATION_MARGIN:-30}"
ALGOS="${ALGOS:-classic pid qpid champion}"

if [ "$REPORT_ONLY" = "1" ] && [ "$NO_REPORT" = "1" ]; then
  echo "error: --report-only and --no-report are mutually exclusive" >&2
  exit 2
fi

# ---- run phase: every algo x scenario, one CSV per run --------------------
run_benchmark() {
  if [ ! -x "$BIN" ]; then
    echo "==> building release vardiff-sim (not found at $BIN)"
    cargo build --release --bin vardiff-sim || {
      echo "build failed" >&2
      exit 1
    }
  fi

  local scenarios="${SCENARIOS:-}"
  if [ -z "$scenarios" ]; then
    scenarios="$(ls scenarios/*.toml | xargs -n1 basename | sed 's/\.toml//')"
  fi

  mkdir -p "$OUTDIR"
  : >"$OUTDIR/_progress.log"
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
  local scen algo
  for scen in $scenarios; do
    is_heavy "$scen" || continue
    for algo in $ALGOS; do
      while [ "$(jobs_running)" -ge 2 ]; do sleep 2; done
      run_one "$algo" "$scen" &
      sleep 8
    done
  done
  wait

  # Pass 2: the rest, full concurrency.
  for algo in $ALGOS; do
    for scen in $scenarios; do
      is_heavy "$scen" && continue
      while [ "$(jobs_running)" -ge "$MAX_PARALLEL" ]; do sleep 2; done
      run_one "$algo" "$scen" &
      sleep 4
    done
  done
  wait

  local n_ok n_all
  n_ok="$(grep -c 'exit=0 ' "$OUTDIR/_progress.log" || true)"
  n_all="$(grep -c 'exit=' "$OUTDIR/_progress.log" || true)"
  echo "ALL DONE $(date -u +%H:%M:%S)  ($n_ok/$n_all runs exit=0)" | tee -a "$OUTDIR/_progress.log"
  echo "CSVs: $(ls "$OUTDIR"/*.csv 2>/dev/null | wc -l) in $OUTDIR"
}

# ---- report phase: build the HTML + analysis.json -------------------------
build_report() {
  echo "==> building report generator (release)"
  cargo build --release --bin vardiff-bench-report || {
    echo "report build failed" >&2
    exit 1
  }
  echo "==> generating report -> $REPORT"
  "$IT_DIR/target/release/vardiff-bench-report" "$OUTDIR" "$REPORT" || {
    echo "report generation failed" >&2
    exit 1
  }
  echo "==> done"
  echo "    report:   $REPORT"
  echo "    analysis: ${REPORT%/*}/analysis.json"
  echo "    (raw CSVs in $OUTDIR are gitignored; commit the HTML + analysis.json)"
}

# ---- orchestrate ----------------------------------------------------------
if [ "$REPORT_ONLY" = "1" ]; then
  echo "==> REPORT_ONLY: skipping build+run, using existing CSVs in $OUTDIR"
  [ -n "$(ls "$OUTDIR"/*.csv 2>/dev/null)" ] || {
    echo "error: no CSVs in $OUTDIR — run without --report-only first" >&2
    exit 1
  }
  build_report
  exit 0
fi

# Full pipeline eagerly (re)builds the sim so the run reflects the latest code;
# --no-report mirrors the old run-benchmark.sh, which built only when missing
# (run_benchmark's own guard handles that).
if [ "$NO_REPORT" != "1" ]; then
  echo "==> building vardiff-sim (release)"
  cargo build --release --bin vardiff-sim || {
    echo "build failed" >&2
    exit 1
  }
fi

echo "==> running benchmark -> $OUTDIR"
run_benchmark

if [ "$NO_REPORT" = "1" ]; then
  echo "next: integration-tests/benchmark/benchmark.sh --report-only $OUTDIR"
  exit 0
fi

build_report
