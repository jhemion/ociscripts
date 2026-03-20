#!/bin/bash
set -euo pipefail

# ====== USER-CONFIGURABLE VARIABLES ======
OUTPUT_DIR="${OUTPUT_DIR:-/lfs/wkld-rep/all-write/}"  # Update the Dir Output
NUM_FILES="${NUM_FILES:-3000000}"      # Total files to create in OUTPUT_DIR
REPORT_EVERY="${REPORT_EVERY:-30000}"  # How often to print a progress report
CHARSET='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
CHARS_PER_FILE="${CHARS_PER_FILE:-1023}"
PROGRESS_FLUSH_EVERY="${PROGRESS_FLUSH_EVERY:-100}"
MAX_FILES_PER_SECOND="${MAX_FILES_PER_SECOND:-0}"  # 0 disables throttling
REPORT_POLL_INTERVAL="${REPORT_POLL_INTERVAL:-0.1}"
THROTTLE_START_DELAY_NS=100000000

if command -v nproc >/dev/null 2>&1; then
  DEFAULT_THREADS="$(nproc)"
elif command -v getconf >/dev/null 2>&1; then
  DEFAULT_THREADS="$(getconf _NPROCESSORS_ONLN)"
else
  DEFAULT_THREADS="1"
fi
THREADS="${THREADS:-$DEFAULT_THREADS}"
# =========================================

echo "Creating $NUM_FILES files in $OUTPUT_DIR"
echo "..."

mkdir -p "$OUTPUT_DIR"

# Validate inputs
for varname in THREADS NUM_FILES REPORT_EVERY CHARS_PER_FILE PROGRESS_FLUSH_EVERY; do
  value="$(eval "printf '%s' \"\${$varname}\"" | tr -d '[:space:]')"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
    echo "ERROR: $varname must be a positive integer." >&2
    exit 1
  fi
  printf -v "$varname" '%s' "$value"
done

value="$(printf '%s' "$MAX_FILES_PER_SECOND" | tr -d '[:space:]')"
if ! [[ "$value" =~ ^[0-9]+$ ]]; then
  echo "ERROR: MAX_FILES_PER_SECOND must be a non-negative integer." >&2
  exit 1
fi
printf -v MAX_FILES_PER_SECOND '%s' "$value"

START_TS=$(date +%s)
NEXT_REPORT=$REPORT_EVERY
LAST_REPORT_COUNT=0
REPORT_START_TS=$START_TS
RUN_ID="$(date +%s)_$$"

COUNTS_DIR="$(mktemp -d)"
PIDS=()

cleanup() {
  rm -rf "$COUNTS_DIR"
}
trap cleanup EXIT

worker() {
  local worker_id="$1"
  local dir="$2"
  local count_file="$3"
  local idx
  local produced=0
  local filename

  for ((idx = worker_id; idx <= NUM_FILES; idx += THREADS)); do
    if (( MAX_FILES_PER_SECOND > 0 )); then
      wait_for_throttle_slot "$idx"
    fi

    filename="${dir}/unique_file_${RUN_ID}_${idx}.txt"
    (
      set +o pipefail
      LC_ALL=C tr -dc "$CHARSET" < /dev/urandom | head -c "$CHARS_PER_FILE" > "$filename"
    )
    produced=$((produced + 1))

    if (( produced % PROGRESS_FLUSH_EVERY == 0 || idx + THREADS > NUM_FILES )); then
      printf '%s\n' "$produced" > "$count_file"
    fi
  done
}

wait_for_throttle_slot() {
  local idx="$1"
  local target_ns
  local now_ns
  local sleep_ns
  local sleep_secs

  target_ns=$((THROTTLE_START_NS + (((idx - 1) * 1000000000) / MAX_FILES_PER_SECOND)))
  while true; do
    now_ns="$(date +%s%N)"
    if (( now_ns >= target_ns )); then
      return 0
    fi

    sleep_ns=$((target_ns - now_ns))
    printf -v sleep_secs '%d.%09d' $((sleep_ns / 1000000000)) $((sleep_ns % 1000000000))
    sleep "$sleep_secs"
  done
}

sum_counts() {
  local total=0
  local count_file
  local value

  for count_file in "$COUNTS_DIR"/*.count; do
    read -r value < "$count_file"
    total=$((total + value))
  done

  printf '%s\n' "$total"
}

echo "Writing files..."

if (( MAX_FILES_PER_SECOND > 0 )); then
  THROTTLE_START_NS=$(( $(date +%s%N) + THROTTLE_START_DELAY_NS ))
fi

if (( THREADS > NUM_FILES )); then
  THREADS="$NUM_FILES"
fi

for ((worker_id = 1; worker_id <= THREADS; worker_id++)); do
  count_file="${COUNTS_DIR}/${worker_id}.count"
  printf '0\n' > "$count_file"

  worker "$worker_id" "$OUTPUT_DIR" "$count_file" &
  PIDS+=("$!")
done

# Reporting loop
while true; do
  count="$(sum_counts)"

  if (( count >= NEXT_REPORT || count >= NUM_FILES )); then
    NOW_TS=$(date +%s)
    DELTA=$((NOW_TS - REPORT_START_TS))
    (( DELTA <= 0 )) && DELTA=1
    COMPLETED_SINCE_LAST=$((count - LAST_REPORT_COUNT))
    AVG=$(awk -v f="$COMPLETED_SINCE_LAST" -v t="$DELTA" 'BEGIN { printf "%.2f", f/t }')
    echo "=== PROGRESS ==="
    echo " Files created: $count"
    echo " Time taken (s) since last report: $DELTA"
    echo " Avg files per second since last: $AVG"
    echo "================"
    REPORT_START_TS=$NOW_TS
    LAST_REPORT_COUNT="$count"
    while (( count >= NEXT_REPORT )); do
      NEXT_REPORT=$((NEXT_REPORT + REPORT_EVERY))
    done
  fi

  if (( count >= NUM_FILES )); then
    break
  fi

  any_alive=0
  for pid in "${PIDS[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      any_alive=1
      break
    fi
  done

  if (( any_alive == 0 )); then
    count="$(sum_counts)"
    if (( count >= NUM_FILES )); then
      break
    fi
    echo "ERROR: Workers exited before all files were created." >&2
    break
  fi

  sleep "$REPORT_POLL_INTERVAL"
done

for pid in "${PIDS[@]}"; do
  wait "$pid"
done

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))
(( ELAPSED <= 0 )) && ELAPSED=1
TOTAL_FILES="$NUM_FILES"
AVG=$(awk -v f="$TOTAL_FILES" -v t="$ELAPSED" 'BEGIN { printf "%.2f", f/t }')

echo "==== FINAL SUMMARY ===="
echo "Output directory: $OUTPUT_DIR"
echo "Threads used: $THREADS"
echo "Total files written: $TOTAL_FILES"
echo "Time taken (s): $ELAPSED"
echo "Avg files per second: $AVG"
