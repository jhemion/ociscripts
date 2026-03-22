#!/usr/bin/env bash
set -euo pipefail
# Usage:
#   ./move-files-forever.sh /src/dir /dst/dir [--overwrite] [--threads N|--threads=N]
#                           [--scanEverySeconds N|--scanEverySeconds=N]
#                           [--stableSeconds N|--stableSeconds=N]
#
# Logs both Java output and rate checkpoints to:
#   ~/log-of-speeds-move-files-forever.txt
#
# NOTE: This is continuous; it will not exit on its own.

SRC="${1:-}"
DST="${2:-}"
shift 2 || true

if [[ -z "${SRC}" || -z "${DST}" ]]; then
  echo "Usage: $0 <sourceDir> <targetDir> [--overwrite] [--threads N] [--scanEverySeconds N] [--stableSeconds N]" >&2
  exit 2
fi

OVERWRITE=""
THREADS="32"
SCAN_EVERY_SECONDS="60"
STABLE_SECONDS="30"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --overwrite)
      OVERWRITE="--overwrite"
      shift
      ;;
    --threads)
      THREADS="${2:-}"
      [[ -n "${THREADS}" ]] || { echo "Missing value after --threads" >&2; exit 2; }
      shift 2
      ;;
    --threads=*)
      THREADS="${1#--threads=}"
      shift
      ;;
    --scanEverySeconds)
      SCAN_EVERY_SECONDS="${2:-}"
      [[ -n "${SCAN_EVERY_SECONDS}" ]] || { echo "Missing value after --scanEverySeconds" >&2; exit 2; }
      shift 2
      ;;
    --scanEverySeconds=*)
      SCAN_EVERY_SECONDS="${1#--scanEverySeconds=}"
      shift
      ;;
    --stableSeconds)
      STABLE_SECONDS="${2:-}"
      [[ -n "${STABLE_SECONDS}" ]] || { echo "Missing value after --stableSeconds" >&2; exit 2; }
      shift 2
      ;;
    --stableSeconds=*)
      STABLE_SECONDS="${1#--stableSeconds=}"
      shift
      ;;
    *)
      echo "Unknown arg: $1" >&2
      exit 2
      ;;
  esac
done

REPORT_EVERY=30000
LOG_FILE="${HOME}/log-of-speeds-move-files-forever.txt"
: > "${LOG_FILE}"

now_ns() { date +%s%N; }
cur_ts() { date +"%Y-%m-%d %H:%M:%S"; }
logln() { echo "$*" | tee -a "${LOG_FILE}"; }

START_NS="$(now_ns)"
LAST_NS="$START_NS"
LAST_COUNT=0

logln "Starting move-files-forever at: $(date)"
logln "Threads: ${THREADS}"
logln "Reporting average files/sec every ${REPORT_EVERY} files."
logln "scanEverySeconds: ${SCAN_EVERY_SECONDS}"
logln "stableSeconds: ${STABLE_SECONDS}"
logln "Logging to: ${LOG_FILE}"
logln ""

# Assumes the compiled class is on the classpath (current directory).
# If needed, add: -cp /path/to/classes
sudo java MoveFilesForever "${SRC}" "${DST}" ${OVERWRITE} \
  --threads="${THREADS}" \
  --progressEvery="${REPORT_EVERY}" \
  --scanEverySeconds="${SCAN_EVERY_SECONDS}" \
  --stableSeconds="${STABLE_SECONDS}" 2>&1 | \
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
done
