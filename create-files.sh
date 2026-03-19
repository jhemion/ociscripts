#!/bin/bash
set -euo pipefail

# ====== USER-CONFIGURABLE VARIABLES ======
OUTPUT_DIR="/lfs/wkld-rep/all-write/" # Update the Dir Output
NUM_FILES="${NUM_FILES:-3000000}"     # Total files to create in OUTPUT_DIR
REPORT_EVERY="${REPORT_EVERY:-30000}"  # How often to print a progress report
CHARSET='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
CHARS_PER_FILE=1023

if command -v nproc >/dev/null 2>&1; then
  DEFAULT_THREADS="$(nproc)"
else
  DEFAULT_THREADS="1"
fi
THREADS="${THREADS:-$DEFAULT_THREADS}"
# =========================================

echo "Creating $NUM_FILES files in $OUTPUT_DIR"
echo "..."

mkdir -p "$OUTPUT_DIR"

# Validate inputs
for varname in THREADS NUM_FILES REPORT_EVERY; do
  value="$(eval echo \$$varname | tr -d '[:space:]')"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
    echo "ERROR: $varname must be a positive integer." >&2
    exit 1
  fi
  eval "$varname=\"$value\""
done

START_TS=$(date +%s)
CUR_TS=$START_TS
NEXT_REPORT=$REPORT_EVERY

# Shared file counter file (for parallel safety)
COUNTER_FILE=$(mktemp)
echo 0 > "$COUNTER_FILE"

write_one_file() {
  local idx="$1"
  local dir="$2"
  local filename="$dir/unique_file_$(date +%s%N)_$idx.txt"
  # LC_ALL=C ensures consistent byte/character behavior.
  LC_ALL=C tr -dc "$CHARSET" < /dev/urandom | head -c "$CHARS_PER_FILE" > "$filename"
  # Atomically increment file counter (for reporting)
  (
    flock -x 200
    count=$(<"$COUNTER_FILE")
    count=$((count + 1))
    echo "$count" > "$COUNTER_FILE"
  ) 200>"$COUNTER_FILE".lock
}
export -f write_one_file
export CHARSET CHARS_PER_FILE COUNTER_FILE

echo "Writing files..."

REPORT_START_TS=$START_TS
seq 1 "$NUM_FILES" | xargs -n 1 -P "$THREADS" -I{} bash -c 'write_one_file "$@"' _ {} "$OUTPUT_DIR" &

# Reporting loop
while true; do
  sleep 1
  count=$(<"$COUNTER_FILE")
  if (( count >= NEXT_REPORT || count >= NUM_FILES )); then
    NOW_TS=$(date +%s)
    DELTA=$((NOW_TS - REPORT_START_TS))
    DELTA=$((DELTA<=0 ? 1 : DELTA))
    num_since_last=$(( count >= NEXT_REPORT ? REPORT_EVERY : (count % REPORT_EVERY)))
    ((num_since_last == 0)) && num_since_last=$REPORT_EVERY
    AVG=$(awk -v f="$num_since_last" -v t="$DELTA" 'BEGIN { printf "%.2f", f/t }')
    echo "=== PROGRESS ==="
    echo " Files created: $count"
    echo " Time taken (s) since last report: $DELTA"
    echo " Avg files per second since last: $AVG"
    echo "================"
    REPORT_START_TS=$NOW_TS
    NEXT_REPORT=$((NEXT_REPORT + REPORT_EVERY))
  fi
  if (( count >= NUM_FILES )); then
    break
  fi
done

wait

END_TS=$(date +%s)
ELAPSED=$(( END_TS - START_TS ))
(( ELAPSED <= 0 )) && ELAPSED=1
TOTAL_FILES="$NUM_FILES"
AVG=$(awk -v f="$TOTAL_FILES" -v t="$ELAPSED" 'BEGIN { printf "%.2f", f/t }')

echo "==== FINAL SUMMARY ===="
echo "Output directory: $OUTPUT_DIR"
echo "Threads used: $THREADS"
echo "Total files written: $TOTAL_FILES"
echo "Time taken (s): $ELAPSED"
echo "Avg files per second: $AVG"

rm -f "$COUNTER_FILE" "$COUNTER_FILE.lock"