#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_move_all_multithread_with_rate.sh /src/dir /dst/dir [--overwrite] [--threads N|--threads=N]
#
# Logs both Java output and rate checkpoints to:
#   ~/log-of-speeds-multithread.txt

SRC="${1:-}"
DST="${2:-}"
shift 2 || true

if [[ -z "${SRC}" || -z "${DST}" ]]; then
  echo "Usage: $0 <sourceDir> <targetDir> [--overwrite] [--threads N|--threads=N]" >&2
  exit 2
fi

OVERWRITE=""
THREADS="32"

# Parse remaining args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --overwrite)
      OVERWRITE="--overwrite"
      shift
      ;;
    --threads)
      THREADS="${2:-}"
      if [[ -z "${THREADS}" ]]; then
        echo "Missing value after --threads" >&2
        exit 2
      fi
      shift 2
      ;;
    --threads=*)
      THREADS="${1#--threads=}"
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

REPORT_EVERY=30000
LOG_FILE="${HOME}/log-of-speeds-multithread.txt"

: > "${LOG_FILE}"

now_ns() { date +%s%N; }
cur_ts() { date +"%Y-%m-%d %H:%M:%S"; }
logln() { echo "$*" | tee -a "${LOG_FILE}"; }

START_NS="$(now_ns)"
LAST_NS="$START_NS"
LAST_COUNT=0

logln "Starting MOVE-ALL at: $(date)"
logln "Threads: ${THREADS}"
logln "Reporting average files/sec every ${REPORT_EVERY} files."
logln "Logging to: ${LOG_FILE}"
logln ""

sudo java MoveFilesNioMultiThreadAll "${SRC}" "${DST}" ${OVERWRITE} \
  --threads="${THREADS}" \
  --progressEvery="${REPORT_EVERY}" 2>&1 | \
while IFS= read -r line; do
  echo "$line" | tee -a "${LOG_FILE}"

  if [[ "$line" =~ ^Moved[[:space:]]+([0-9]+)[[:space:]]+files ]]; then
    count="${BASH_REMATCH[1]}"
    if (( count > 0 && count % REPORT_EVERY == 0 )); then
      NOW_NS="$(now_ns)"
      delta_ns=$((NOW_NS - LAST_NS))
      delta_count=$((count - LAST_COUNT))
      overall_ns=$((NOW_NS - START_NS))

      interval_rate="$(awk -v c="$delta_count" -v ns="$delta_ns" 'BEGIN { if (ns>0) printf "%.2f", c/(ns/1e9); else print "inf" }')"
      overall_rate="$(awk -v c="$count" -v ns="$overall_ns" 'BEGIN { if (ns>0) printf "%.2f", c/(ns/1e9); else print "inf" }')"
      interval_secs="$(awk -v ns="$delta_ns" 'BEGIN { printf "%.2f", ns/1e9 }')"
      overall_secs="$(awk -v ns="$overall_ns" 'BEGIN { printf "%.2f", ns/1e9 }')"
      cur_time="$(cur_ts)"

      logln "=== RATE CHECKPOINT @ ${count} files ==="
      logln "${cur_time} - Last ${delta_count} files: ${interval_rate} files/sec (over ${interval_secs}s)"
      logln "${cur_time} - Overall ${count} files: ${overall_rate} files/sec (over ${overall_secs}s)"
      logln "======================================="
      logln ""

      LAST_NS="$NOW_NS"
      LAST_COUNT="$count"
    fi
  fi

  if [[ "$line" =~ ^Done\.\ Moved=([0-9]+), ]]; then
    final_count="${BASH_REMATCH[1]}"
    END_NS="$(now_ns)"
    total_ns=$((END_NS - START_NS))
    total_rate="$(awk -v c="$final_count" -v ns="$total_ns" 'BEGIN { if (ns>0) printf "%.2f", c/(ns/1e9); else print "inf" }')"
    total_secs="$(awk -v ns="$total_ns" 'BEGIN { printf "%.2f", ns/1e9 }')"
    logln ""
    logln "FINAL: ${final_count} files moved in ${total_secs}s => ${total_rate} files/sec average"
  fi
done