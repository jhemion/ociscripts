#!/bin/bash
set -euo pipefail

# ====== USER-CONFIGURABLE VARIABLES ======
OUTPUT_DIR="/lfs-iad/lfs-iad-lfs-phx/d"
NUM_FILES="${NUM_FILES:-3000000}"          # Total files to create in OUTPUT_DIR
REPORT_EVERY="${REPORT_EVERY:-30000}"      # Progress report interval (kept for compatibility; batch reports at PAUSE_EVERY)
PAUSE_EVERY="${PAUSE_EVERY:-30000}"        # Pause after this many files (batch size)
PAUSE_SECONDS="${PAUSE_SECONDS:-60}"       # Pause duration in seconds
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
echo "Threads: $THREADS"
echo "Pause: ${PAUSE_SECONDS}s every ${PAUSE_EVERY} files"
echo "..."

mkdir -p "$OUTPUT_DIR"

# Validate inputs
for varname in THREADS NUM_FILES REPORT_EVERY PAUSE_EVERY PAUSE_SECONDS; do
	  value="$(eval echo \$$varname | tr -d '[:space:]')"
	    if ! [[ "$value" =~ ^[0-9]+$ ]] || (( value < 1 )); then
		        echo "ERROR: $varname must be a positive integer." >&2
			    exit 1
			      fi
			        eval "$varname=\"$value\""
			done

			START_TS=$(date +%s)
			ACTIVE_TOTAL=0

			# Shared file counter file (for parallel safety)
			COUNTER_FILE=$(mktemp)
			echo 0 > "$COUNTER_FILE"

			write_one_file() {
				  local idx="$1"
				    local dir="$2"
				      local filename="$dir/unique_file_$(date +%s%N)_$idx.txt"
				        LC_ALL=C tr -dc "$CHARSET" < /dev/urandom | head -c "$CHARS_PER_FILE" > "$filename"
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

					    CREATED_BEFORE_BATCH=0

					    # Create files in batches so we can truly pause creation
					    for (( batch_start=1; batch_start<=NUM_FILES; batch_start+=PAUSE_EVERY )); do
						      batch_end=$((batch_start + PAUSE_EVERY - 1))
						        (( batch_end > NUM_FILES )) && batch_end=$NUM_FILES
							  batch_size=$((batch_end - batch_start + 1))

							    echo "Starting batch: $batch_start..$batch_end (size=$batch_size)"

							      # Measure ONLY active creation time (exclude pause time)
							        BATCH_ACTIVE_START_TS=$(date +%s)

								  seq "$batch_start" "$batch_end" | xargs -n 1 -P "$THREADS" -I{} bash -c 'write_one_file "$@"' _ {} "$OUTPUT_DIR"
								    wait

								      BATCH_ACTIVE_END_TS=$(date +%s)
								        ACTIVE_DELTA=$((BATCH_ACTIVE_END_TS - BATCH_ACTIVE_START_TS))
									  (( ACTIVE_DELTA <= 0 )) && ACTIVE_DELTA=1
									    ACTIVE_TOTAL=$((ACTIVE_TOTAL + ACTIVE_DELTA))

									      # Progress report (rate excludes the sleep)
									        count=$(<"$COUNTER_FILE")
										  num_since_last=$((count - CREATED_BEFORE_BATCH))
										    (( num_since_last <= 0 )) && num_since_last=$batch_size

										      AVG=$(awk -v f="$num_since_last" -v t="$ACTIVE_DELTA" 'BEGIN { printf "%.2f", f/t }')

										        echo "=== PROGRESS ==="
											  echo " Files created: $count"
											    echo " Active time (s) since last report: $ACTIVE_DELTA"
											      echo " Avg files per second (active only): $AVG"
											        echo "================"

												  CREATED_BEFORE_BATCH=$count

												    # Pause between batches (unless we're done)
												      if (( count < NUM_FILES )); then
													          echo "Pausing ${PAUSE_SECONDS}s after ${count} files..."
													      sleep "$PAUSE_SECONDS"
													        fi
													done

													END_TS=$(date +%s)
													ELAPSED=$(( END_TS - START_TS ))
													(( ELAPSED <= 0 )) && ELAPSED=1
													(( ACTIVE_TOTAL <= 0 )) && ACTIVE_TOTAL=1

													TOTAL_FILES="$NUM_FILES"
													AVG=$(awk -v f="$TOTAL_FILES" -v t="$ACTIVE_TOTAL" 'BEGIN { printf "%.2f", f/t }')

													echo "==== FINAL SUMMARY ===="
													echo "Output directory: $OUTPUT_DIR"
													echo "Threads used: $THREADS"
													echo "Total files written: $TOTAL_FILES"
													echo "Time taken (s) wall clock (includes pauses): $ELAPSED"
													echo "Time taken (s) active only (excludes pauses): $ACTIVE_TOTAL"
													echo "Avg files per second (active only): $AVG"

													rm -f "$COUNTER_FILE" "$COUNTER_FILE.lock"