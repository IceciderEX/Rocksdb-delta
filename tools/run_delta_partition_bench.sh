#!/usr/bin/env bash
# DELTA_PARTITION_OBSERVABILITY_OR_TEST_ONLY: ad-hoc benchmark wrapper for
# comparing baseline vs Delta L0 partition behavior.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-$REPO_ROOT/build}"
DB_ROOT="${DB_ROOT:-$REPO_ROOT/db_perf_test/delta_partition_bench}"
LOG_DIR="${LOG_DIR:-$DB_ROOT/logs}"
TOOL_PATH="$BUILD_DIR/delta_partition_bench"

mkdir -p "$LOG_DIR"

cmake -S "$REPO_ROOT" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DWITH_BENCHMARK_TOOLS=ON
env CCACHE_DISABLE=1 cmake --build "$BUILD_DIR" --target delta_partition_bench -j4

COMMON_ARGS=(
  "--db_root=$DB_ROOT"
  "--target_seconds=${TARGET_SECONDS:-6}"
  "--min_passes=${MIN_PASSES:-1}"
  "--num_tables=${NUM_TABLES:-96}"
  "--cuids_per_table=${CUIDS_PER_TABLE:-2}"
  "--insert_rows_per_cuid=${INSERT_ROWS_PER_CUID:-64}"
  "--insert_chunk_rows=${INSERT_CHUNK_ROWS:-8}"
  "--update_merge_rounds=${UPDATE_MERGE_ROUNDS:-2}"
  "--updates_per_merge=${UPDATES_PER_MERGE:-96}"
  "--update_chunk_ops=${UPDATE_CHUNK_OPS:-8}"
  "--user_scans_per_insert_chunk=${USER_SCANS_PER_INSERT_CHUNK:-1}"
  "--user_scans_per_update_chunk=${USER_SCANS_PER_UPDATE_CHUNK:-2}"
  "--range_scan_len=${RANGE_SCAN_LEN:-8}"
  "--value_size=${VALUE_SIZE:-128}"
  "--seed=${SEED:-7}"
)

BASELINE_LOG="$LOG_DIR/baseline.log"
DELTA_LOG="$LOG_DIR/delta.log"

"$TOOL_PATH" --mode=baseline "${COMMON_ARGS[@]}" | tee "$BASELINE_LOG"
"$TOOL_PATH" --mode=delta "${COMMON_ARGS[@]}" | tee "$DELTA_LOG"

baseline_total_seconds="$(awk -F= '/^total_seconds=/{print $2}' "$BASELINE_LOG")"
delta_total_seconds="$(awk -F= '/^total_seconds=/{print $2}' "$DELTA_LOG")"
baseline_write_ops="$(awk -F= '/^write_ops=/{print $2}' "$BASELINE_LOG")"
delta_write_ops="$(awk -F= '/^write_ops=/{print $2}' "$DELTA_LOG")"
baseline_point_ops="$(awk -F= '/^point_get_ops=/{print $2}' "$BASELINE_LOG")"
delta_point_ops="$(awk -F= '/^point_get_ops=/{print $2}' "$DELTA_LOG")"
baseline_scan_ops="$(awk -F= '/^user_scan_ops=/{print $2}' "$BASELINE_LOG")"
delta_scan_ops="$(awk -F= '/^user_scan_ops=/{print $2}' "$DELTA_LOG")"
baseline_write_qps="$(awk -F= '/^write_qps=/{print $2}' "$BASELINE_LOG")"
delta_write_qps="$(awk -F= '/^write_qps=/{print $2}' "$DELTA_LOG")"
baseline_point_qps="$(awk -F= '/^point_get_qps=/{print $2}' "$BASELINE_LOG")"
delta_point_qps="$(awk -F= '/^point_get_qps=/{print $2}' "$DELTA_LOG")"
baseline_scan_qps="$(awk -F= '/^user_scan_qps=/{print $2}' "$BASELINE_LOG")"
delta_scan_qps="$(awk -F= '/^user_scan_qps=/{print $2}' "$DELTA_LOG")"
delta_get_before="$(awk -F= '/^delta_l0_stats.get_l0_candidate_files_before_prune=/{print $2}' "$DELTA_LOG")"
delta_get_after="$(awk -F= '/^delta_l0_stats.get_l0_candidate_files_after_prune=/{print $2}' "$DELTA_LOG")"
delta_scan_before="$(awk -F= '/^delta_l0_stats.scan_l0_candidate_files_before_prune=/{print $2}' "$DELTA_LOG")"
delta_scan_after="$(awk -F= '/^delta_l0_stats.scan_l0_candidate_files_after_prune=/{print $2}' "$DELTA_LOG")"

python3 - "$baseline_total_seconds" "$delta_total_seconds" \
  "$baseline_write_ops" "$delta_write_ops" \
  "$baseline_point_ops" "$delta_point_ops" \
  "$baseline_scan_ops" "$delta_scan_ops" \
  "$baseline_write_qps" "$delta_write_qps" \
  "$baseline_point_qps" "$delta_point_qps" \
  "$baseline_scan_qps" "$delta_scan_qps" \
  "$delta_get_before" "$delta_get_after" \
  "$delta_scan_before" "$delta_scan_after" <<'PY'
import sys

baseline_total_seconds = float(sys.argv[1])
delta_total_seconds = float(sys.argv[2])
baseline_write_ops = float(sys.argv[3])
delta_write_ops = float(sys.argv[4])
baseline_point_ops = float(sys.argv[5])
delta_point_ops = float(sys.argv[6])
baseline_scan_ops = float(sys.argv[7])
delta_scan_ops = float(sys.argv[8])
baseline_write_qps = float(sys.argv[9])
delta_write_qps = float(sys.argv[10])
baseline_point_qps = float(sys.argv[11])
delta_point_qps = float(sys.argv[12])
baseline_scan_qps = float(sys.argv[13])
delta_scan_qps = float(sys.argv[14])
delta_get_before = float(sys.argv[15] or 0)
delta_get_after = float(sys.argv[16] or 0)
delta_scan_before = float(sys.argv[17] or 0)
delta_scan_after = float(sys.argv[18] or 0)

def improve(base, new):
    if base == 0:
        return 0.0
    return (new - base) / base * 100.0

def reduce(before, after):
    if before == 0:
        return 0.0
    return (before - after) / before * 100.0

def rate(ops, seconds):
    if seconds == 0:
        return 0.0
    return ops / seconds

print("summary.mode_total_seconds_baseline=%.2f" % baseline_total_seconds)
print("summary.mode_total_seconds_delta=%.2f" % delta_total_seconds)
print("summary.write_ops_baseline=%.0f" % baseline_write_ops)
print("summary.write_ops_delta=%.0f" % delta_write_ops)
print("summary.write_ops_per_wall_sec_baseline=%.2f" % rate(baseline_write_ops, baseline_total_seconds))
print("summary.write_ops_per_wall_sec_delta=%.2f" % rate(delta_write_ops, delta_total_seconds))
print("summary.write_ops_improvement_pct=%.2f" % improve(rate(baseline_write_ops, baseline_total_seconds), rate(delta_write_ops, delta_total_seconds)))
print("summary.point_get_ops_baseline=%.0f" % baseline_point_ops)
print("summary.point_get_ops_delta=%.0f" % delta_point_ops)
print("summary.point_get_ops_per_wall_sec_baseline=%.2f" % rate(baseline_point_ops, baseline_total_seconds))
print("summary.point_get_ops_per_wall_sec_delta=%.2f" % rate(delta_point_ops, delta_total_seconds))
print("summary.point_get_ops_improvement_pct=%.2f" % improve(rate(baseline_point_ops, baseline_total_seconds), rate(delta_point_ops, delta_total_seconds)))
print("summary.user_scan_ops_baseline=%.0f" % baseline_scan_ops)
print("summary.user_scan_ops_delta=%.0f" % delta_scan_ops)
print("summary.user_scan_ops_per_wall_sec_baseline=%.2f" % rate(baseline_scan_ops, baseline_total_seconds))
print("summary.user_scan_ops_per_wall_sec_delta=%.2f" % rate(delta_scan_ops, delta_total_seconds))
print("summary.user_scan_ops_improvement_pct=%.2f" % improve(rate(baseline_scan_ops, baseline_total_seconds), rate(delta_scan_ops, delta_total_seconds)))
print("summary.write_qps_baseline=%.2f" % baseline_write_qps)
print("summary.write_qps_delta=%.2f" % delta_write_qps)
print("summary.point_get_qps_baseline=%.2f" % baseline_point_qps)
print("summary.point_get_qps_delta=%.2f" % delta_point_qps)
print("summary.user_scan_qps_baseline=%.2f" % baseline_scan_qps)
print("summary.user_scan_qps_delta=%.2f" % delta_scan_qps)
print("summary.delta_get_prune_reduction_pct=%.2f" % reduce(delta_get_before, delta_get_after))
print("summary.delta_scan_prune_reduction_pct=%.2f" % reduce(delta_scan_before, delta_scan_after))
print("summary.recommended_signal=write_ops+point_get_ops+user_scan_ops+delta_scan_prune_reduction_pct")
PY

echo "logs=$LOG_DIR"
