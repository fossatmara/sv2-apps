#!/usr/bin/env bash
#
# One-shot regeneration of the vardiff wall-time benchmark report, for
# automation. Builds the binaries, runs the full benchmark, and rebuilds the
# committed HTML report — no manual steps.
#
# Usage:
#   integration-tests/benchmark/regenerate.sh
#
# Env overrides (forwarded to run-benchmark.sh; see its header for the rest):
#   SPEED=1            sim clock speed (1 = wall time; the authoritative number)
#   REPORT_ONLY=1      skip the benchmark, just rebuild the report from the
#                      existing CSVs (fast — use after editing the report tool)
#
# Output (committed):   benchmark-report/vardiff-walltime-report.html
#                       benchmark-report/analysis.json
# Output (gitignored):  benchmark-report/raw-csv/  (per-run CSVs, regenerable)
#
# With the in-process mock template provider (no bitcoind), a full 4-algo ×
# 26-scenario run at SPEED=1 is bounded by scenario durations; raise SPEED for
# a faster, less wall-faithful pass.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$IT_DIR"

CSV_DIR="$IT_DIR/benchmark-report/raw-csv"
REPORT="$IT_DIR/benchmark-report/vardiff-walltime-report.html"

if [ "${REPORT_ONLY:-0}" != "1" ]; then
  echo "==> building vardiff-sim (release)"
  cargo build --release --bin vardiff-sim

  echo "==> running benchmark -> $CSV_DIR"
  # run-benchmark.sh defaults SPEED=1; pass through any override.
  SPEED="${SPEED:-1}" "$SCRIPT_DIR/run-benchmark.sh" "$CSV_DIR"
else
  echo "==> REPORT_ONLY: skipping benchmark, using existing CSVs in $CSV_DIR"
  [ -n "$(ls "$CSV_DIR"/*.csv 2>/dev/null)" ] || {
    echo "error: no CSVs in $CSV_DIR — run without REPORT_ONLY first" >&2; exit 1;
  }
fi

echo "==> building report generator (release)"
cargo build --release --bin vardiff-bench-report

echo "==> generating report -> $REPORT"
"$IT_DIR/target/release/vardiff-bench-report" "$CSV_DIR" "$REPORT"

echo "==> done"
echo "    report:   $REPORT"
echo "    analysis: ${REPORT%/*}/analysis.json"
echo "    (raw CSVs in $CSV_DIR are gitignored; commit the HTML + analysis.json)"
