#!/usr/bin/env bash
# Restart test (lab): kill -9 the job between a Bronze write and its checkpoint commit, restart
# from the same checkpoint and drain.
#
#   make restart-test
#
# What it proves: after the restart the Silver has one row per trip_id (zero duplicates), while the
# Bronze, which is append-only, keeps the second write of the uncommitted micro-batch (at-least-once).
#
# Destructive: drops the Silver schema, deletes the Bronze and its checkpoint, recreates the topic,
# rewrites data/state/counters.json and stops nothing — a running stream must be stopped first.
# The report goes to docs/restart-test.txt.
set -euo pipefail
cd "$(dirname "$0")/.."

LIMIT="${LIMIT:-100000}"
OUT="${OUT:-docs/restart-test.txt}"
LOG_DIR="${LOG_DIR:-data/state}"
STREAM_LOG="$LOG_DIR/restart-stream.log"
DRAIN_LOG="$LOG_DIR/restart-drain.log"
PGID_FILE="$LOG_DIR/restart.pgid"
DRAIN_STATUS="$LOG_DIR/restart-drain.status"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-900}"
# The stream consumes trips.raw (the streaming/job.py default); the producer and this script agree.
TOPIC=trips.raw

mkdir -p "$LOG_DIR" docs
exec > >(tee "$OUT") 2>&1

say()    { printf '%s\n' "$*"; }
stamp()  { date -u '+%H:%M:%S UTC'; }
psql_t() { docker compose exec -T postgres psql -U stream -d streaming -tAc "$1"; }

# Prints KEY=VALUE out of the one-line counts (e.g. `rows=1000 files=2`) or the `--check` line.
field() {
  awk -v key="$1" '{ for (i = 1; i <= NF; i++) { split($i, kv, "="); if (kv[1] == key) { print kv[2]; exit } } }' <<<"$2"
}

# The crash can leave a half-written Parquet file; a read failure is retried so the report survives
# the very thing it is testing. An empty result means the Bronze could not be read at all.
bronze_check() {
  local out
  for _ in 1 2 3; do
    out=$(uv run python scripts/count_bronze.py --dir data/bronze --check 2>/dev/null || true)
    if [ -n "$out" ]; then
      printf '%s\n' "$out"
      return 0
    fi
    sleep 1
  done
  return 1
}

# Row total only, same retry: used for the baseline right after the checkpoint commit, when the next
# batch may already be writing a file.
bronze_rows() {
  local out
  for _ in 1 2 3; do
    out=$(uv run python scripts/count_bronze.py --dir data/bronze 2>/dev/null || true)
    out=$(sed -n 's/^rows=\([0-9]*\).*/\1/p' <<<"$out")
    if [ -n "$out" ]; then
      printf '%s\n' "$out"
      return 0
    fi
    sleep 1
  done
  return 1
}

stop_group() {
  [ -n "${1:-}" ] || return 0
  kill -9 -- "-$1" 2>/dev/null || true
  for _ in $(seq 1 100); do
    kill -0 -- "-$1" 2>/dev/null || return 0
    sleep 0.1
  done
  say "warning: process group $1 is still alive after the kill"
}

# Starts `make stream` in its own session and reads the group id from inside the session: the
# wrapper writes its own pid, which is the group id regardless of how setsid forked. No `exec`, so
# the wrapper can also record make's exit status for the drain.
JOB_PGID=""
start_job() {  # log status make-args...
  local log=$1 status=$2
  shift 2
  rm -f "$PGID_FILE" "$status"
  : > "$log"
  setsid bash -c 'echo $$ > "$1"; shift; status=$1; shift; make --no-print-directory "$@"; echo $? > "$status"' \
    _ "$PGID_FILE" "$status" "$@" >"$log" 2>&1 &
  for _ in $(seq 1 100); do [ -s "$PGID_FILE" ] && break; sleep 0.1; done
  JOB_PGID=$(cat "$PGID_FILE")
}

fails=0
ok()       { say "PASS  $*"; }
bad()      { say "FAIL  $*"; fails=$((fails + 1)); }
check_eq() {
  if [ "$2" = "$3" ]; then ok "$1 ($2 = $3)"; else bad "$1 (got '$2', expected '$3')"; fi
}
check_ge() {
  if ! [[ "$2" =~ ^[0-9]+$ ]]; then bad "$1 (got '$2', expected >= $3)"; return; fi
  if [ "$2" -ge "$3" ]; then ok "$1 ($2 >= $3)"; else bad "$1 (got '$2', expected >= $3)"; fi
}

PGID_ALIVE=""
trap 'stop_group "$PGID_ALIVE"' EXIT

say "# Restart test — $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
say
say "host:   $(uname -sr), $(nproc) cores"
say "limit:  $LIMIT events"
say "crash:  while a micro-batch is written to Bronze and not yet committed to the checkpoint"
say

# --- preconditions ---------------------------------------------------------------------------
for cmd in setsid uv docker awk sed pgrep; do
  command -v "$cmd" >/dev/null || { say "error: $cmd is not in PATH"; exit 1; }
done
if pgrep -f 'streaming\.job' >/dev/null; then
  say "error: a stream job is running (pgrep -f streaming.job) — stop it before this test"
  exit 1
fi
if [ "$LIMIT" -le 50000 ]; then
  say "error: LIMIT must be above max-offsets-per-trigger (50000), otherwise the job needs a single"
  say "       micro-batch and there is no uncommitted batch to kill"
  exit 1
fi
for svc in redpanda postgres; do
  docker compose ps --status running --services | grep -qx "$svc" ||
    { say "error: $svc is not running — run 'make up'"; exit 1; }
done
compgen -G "jars/*.jar" >/dev/null ||
  { say "error: no jars in jars/ — run 'make jars' once (needs network)"; exit 1; }

# --- clean slate -----------------------------------------------------------------------------
say "== reset: Bronze, checkpoint, Silver schema and topic"
make --no-print-directory bronze-reset
if docker compose exec -T redpanda rpk topic describe "$TOPIC" >/dev/null 2>&1; then
  docker compose exec -T redpanda rpk topic delete "$TOPIC" >/dev/null ||
    { say "error: could not delete the topic $TOPIC"; exit 1; }
else
  say "(topic $TOPIC did not exist)"
fi
make --no-print-directory silver-reset >/dev/null
make --no-print-directory topic >/dev/null
say "silver schema recreated from scripts/init_silver.sql"
say

# --- replay ----------------------------------------------------------------------------------
say "== produce: $LIMIT events, no throttle"
make --no-print-directory produce RATE=0 LIMIT="$LIMIT"
end=$(docker compose exec -T redpanda rpk topic describe "$TOPIC" -p | awk 'NR > 1 {total += $NF} END {print total + 0}')
check_eq "topic end offsets" "$end" "$LIMIT"
say

# --- run 1: killed mid-stream ----------------------------------------------------------------
say "[$(stamp)] == run 1: stream; kill -9 with a micro-batch written but not committed"
start_job "$STREAM_LOG" "$LOG_DIR/restart-run1.status" stream
pgid=$JOB_PGID
PGID_ALIVE="$pgid"
say "stream process group: $pgid (log: $STREAM_LOG)"

deadline=$((SECONDS + 420))
while ! grep -q 'batch 0: consumed=' "$STREAM_LOG"; do
  kill -0 -- "-$pgid" 2>/dev/null || { say "error: the job died before batch 0"; exit 1; }
  ((SECONDS < deadline)) || { say "error: batch 0 did not finish in 420s"; exit 1; }
  sleep 1
done
for _ in $(seq 1 100); do [ -f data/checkpoints/bronze/commits/0 ] && break; sleep 0.1; done
[ -f data/checkpoints/bronze/commits/0 ] || { say "error: batch 0 has no checkpoint commit"; exit 1; }

# Baseline = what the committed batch left behind. The kill has to land on the *next* batch, so the
# threshold is the measured Bronze, not a fraction of LIMIT.
batch0_silver=$(psql_t "select count(*) from silver.trip_events")
baseline=$(bronze_rows) || { say "error: could not read the Bronze baseline"; exit 1; }
say "[$(stamp)] batch 0 committed: silver=$batch0_silver rows, bronze=$baseline rows"
say "waiting for the next micro-batch to write Parquet"

# Poll the Parquet metadata in-process: the kill has to land between the next batch's write and its
# checkpoint commit (the Silver phase), a window of seconds. A file mid-move is read again.
uv run python - "$baseline" <<'PY'
import sys
import time

import pyarrow.dataset as ds

threshold = int(sys.argv[1])
deadline = time.monotonic() + 300
last_error = None
while time.monotonic() < deadline:
    try:
        rows = ds.dataset("data/bronze", format="parquet", partitioning="hive").count_rows()
    except Exception as exc:  # the file may be mid-move or truncated by the crash
        last_error = exc
        rows = 0
    if rows > threshold:
        print(f"bronze rows={rows} with the checkpoint still at batch 0")
        sys.exit(0)
    time.sleep(0.05)
print(f"error: the next micro-batch never landed in Bronze (last read error: {last_error})", file=sys.stderr)
sys.exit(1)
PY

say "[$(stamp)] kill -9 -- -$pgid"
kill -9 -- "-$pgid" 2>/dev/null || true
wait 2>/dev/null || true
stop_group "$pgid"
PGID_ALIVE=""
rm -f "$PGID_FILE"
group_gone=no
kill -0 -- "-$pgid" 2>/dev/null || group_gone=yes
say "process group gone: $group_gone"
sleep 2

# The killed client leaves the Postgres backend to unwind; wait for the rollback before restarting.
busy=1
for _ in $(seq 1 30); do
  busy=$(psql_t "select count(*) from pg_stat_activity where usename = 'stream' and pid <> pg_backend_pid() and state <> 'idle'")
  [ "$busy" = "0" ] && break
  sleep 1
done

say
say "== state right after the kill (before the restart)"
commits_after_kill=$(ls data/checkpoints/bronze/commits 2>/dev/null | tr '\n' ' ' | sed 's/ $//')
say "checkpoint commits: [$commits_after_kill]"
say "stream log (batch lines):"
sed 's/\r/\n/g' "$STREAM_LOG" | grep -aE 'batch [0-9]+: consumed=' | sed 's/^/  /'
crash_bronze=""
crash_bronze=$(bronze_check) || crash_bronze=""
if [ -n "$crash_bronze" ]; then
  say "bronze: $crash_bronze"
else
  say "bronze: unreadable (a file was left truncated by the crash)"
fi
crash_bronze_rows=$(field rows "$crash_bronze")
crash_silver=$(psql_t "select count(*) from silver.trip_events")
say "silver rows: $crash_silver (batch 0 had $batch0_silver)"
say "silver batch_runs: $(psql_t 'select count(*) from silver.batch_runs')"
say "postgres busy backends after the crash: $busy"
say

# --- run 2: restart from the same checkpoint and drain ---------------------------------------
say "[$(stamp)] == restart: same checkpoint, --available-now (drain and exit)"
start_job "$DRAIN_LOG" "$DRAIN_STATUS" stream STREAM_ARGS=--available-now
drain_pgid=$JOB_PGID
PGID_ALIVE="$drain_pgid"
say "drain process group: $drain_pgid (log: $DRAIN_LOG)"
deadline=$((SECONDS + DRAIN_TIMEOUT))
while [ -n "$drain_pgid" ] && kill -0 -- "-$drain_pgid" 2>/dev/null; do
  if ((SECONDS >= deadline)); then
    say "error: the drain did not finish in ${DRAIN_TIMEOUT}s"
    stop_group "$drain_pgid"
    break
  fi
  sleep 2
done
PGID_ALIVE=""
rm -f "$PGID_FILE"
drain_status=$(cat "$DRAIN_STATUS" 2>/dev/null || echo killed)
say "[$(stamp)] drain finished (exit status $drain_status)"
sed 's/\r/\n/g' "$DRAIN_LOG" | grep -aE 'batch [0-9]+: consumed=' | sed 's/^/  /' || say "  (the drain logged no batch)"
say

# --- final state and the checks ---------------------------------------------------------------
say "== final state"
final_bronze=""
final_bronze=$(bronze_check) || final_bronze=""
[ -n "$final_bronze" ] && say "bronze: $final_bronze"
b_rows=$(field rows "$final_bronze")
b_files=$(field files "$final_bronze")
b_null=$(field trip_id_null "$final_bronze")
b_pairs=$(field offset_pairs "$final_bronze")
b_dupes=$(field offset_duplicates "$final_bronze")

stats=$(psql_t "select count(*) || ' ' || count(distinct trip_id) || ' ' || count(*) filter (where not is_valid_trip) from silver.trip_events")
read -r s_rows s_distinct s_invalid <<<"$stats"
valid=$(psql_t "select count(*) from silver.trip_events where is_valid_trip")
w_sum=$(psql_t "select coalesce(sum(trips), 0) from silver.zone_demand_5min")
w_count=$(psql_t "select count(*) from silver.zone_demand_5min")
batch_runs=$(psql_t "select count(*) from silver.batch_runs")
c_consumed=$(uv run python -c 'import json; print(json.load(open("data/state/counters.json"))["consumed"])')
c_dupes=$(uv run python -c 'import json; print(json.load(open("data/state/counters.json"))["duplicates_dropped"])')

# The replay size is the first batch of the drain: exactly the ranges the crash left uncommitted.
replayed=$(sed 's/\r/\n/g' "$DRAIN_LOG" | grep -aoE 'batch [0-9]+: consumed=[0-9]+' | head -n 1 | sed 's/.*=//' || true)
crash_written=""
if [[ "$crash_bronze_rows" =~ ^[0-9]+$ ]]; then
  crash_written=$((crash_bronze_rows - baseline))
fi

say "silver: rows=$s_rows distinct_trip_id=$s_distinct invalid=$s_invalid valid=$valid"
say "zone window: windows=$w_count sum(trips)=$w_sum"
say "counters: consumed=$c_consumed duplicates_dropped=$c_dupes"
say "silver batch_runs: $batch_runs"
if [ -n "$crash_written" ] && [ "$crash_written" = "$replayed" ]; then
  say "the killed micro-batch had finished its Parquet write: $crash_written of $replayed rows"
elif [ -n "$crash_written" ]; then
  say "the killed micro-batch was still writing Parquet: $crash_written of ${replayed:-?} rows"
fi
say

say "== checks"
check_eq "checkpoint stopped at batch 0 (the kill landed on the next batch)" "$commits_after_kill" "0"
check_ge "bronze after the kill above batch 0 (the next micro-batch started writing)" \
  "$crash_bronze_rows" "$((baseline + 1))"
check_eq "process group gone after the kill" "$group_gone" "yes"
check_eq "the killed batch left nothing in Silver (crash before its commit)" \
  "$crash_silver" "$batch0_silver"
check_eq "drain exit status" "$drain_status" "0"
check_eq "silver rows = distinct trip_id (zero duplicates)" "$s_rows" "$s_distinct"
check_eq "silver rows = events sent" "$s_rows" "$LIMIT"
check_eq "counters consumed = events sent" "$c_consumed" "$LIMIT"
check_eq "counters duplicates_dropped = 0 (the retry re-inserted every row)" "$c_dupes" "0"
check_eq "bronze offset pairs = events sent (no event lost)" "$b_pairs" "$LIMIT"
check_ge "bronze duplication > 0 (the at-least-once happened)" "$b_dupes" "1"
check_ge "bronze rows > events sent" "$b_rows" "$((LIMIT + 1))"
check_eq "zone window sum = valid trips" "$w_sum" "$valid"
say
say "bronze files: $b_files, null trip_id: $b_null"
say "bronze duplication measured: $b_dupes rows (the micro-batch the crash did not checkpoint)"
say "counter duplicates_dropped: $c_dupes (only the events duplicated inside the replay itself)"
say

if [ "$fails" -eq 0 ]; then
  say "VERDICT: PASS — Silver is idempotent under kill -9; Bronze keeps the at-least-once write."
else
  say "VERDICT: FAIL — $fails check(s) failed."
fi
exit "$fails"
