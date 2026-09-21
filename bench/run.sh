#!/usr/bin/env bash
# Throughput benchmark: replay N hours of synthetic readings for every real
# station through the SAME Flink aggregation as production, and time it.
#
#   sh bench/run.sh [hours] [parallelism]   (defaults 72, 1; needs the stack running)
#
# Writes bench/results.json and prints a markdown row for the README.
set -eu
cd "$(dirname "$0")/.."
export MSYS_NO_PATHCONV=1                     # Git Bash: keep /opt/... paths intact
HOURS=${1:-72}
PAR=${2:-1}
TOPIC=aqi.readings.bench
PY=${PY:-.venv/Scripts/python.exe}; [ -x "$PY" ] || PY=python
PSQL="docker exec -i airpulse-postgres psql -U airpulse -d airpulse -Atq"
REST=http://localhost:8081

echo "== 1/5 fresh topic + sink table"
docker exec airpulse-redpanda rpk topic delete $TOPIC >/dev/null 2>&1 || true
docker exec airpulse-redpanda rpk topic create $TOPIC -p 6 >/dev/null
$PSQL <<'SQL'
DROP TABLE IF EXISTS bench_station_aqi_hourly;
CREATE TABLE bench_station_aqi_hourly (LIKE station_aqi_hourly INCLUDING ALL);
SQL

echo "== 2/5 load generator ($HOURS h)"
GEN=$("$PY" -m src.loadgen --hours "$HOURS" --topic $TOPIC 2>/dev/null | tail -1)
N=$(echo "$GEN" | sed -n 's/.*"messages": \([0-9]*\).*/\1/p')
echo "   $GEN"

echo "== 3/5 derive bench job from flink/sql/station_aqi.sql and submit"
# Job 1 only (up to the JOB 2 marker), pointed at the bench topic and sink.
sed '/-------------------------------- JOB 2/,$d' flink/sql/station_aqi.sql \
  | sed "s/'aqi.readings.raw'/'$TOPIC'/; s/airpulse-flink-station-aqi/airpulse-flink-bench/; \
         s/CREATE TABLE station_aqi_hourly/CREATE TABLE bench_station_aqi_hourly/; \
         s/'table-name' = 'station_aqi_hourly'/'table-name' = 'bench_station_aqi_hourly'/; \
         s/INSERT INTO station_aqi_hourly/INSERT INTO bench_station_aqi_hourly/"   | sed "1i SET 'parallelism.default' = '$PAR';" > bench/_job.sql
docker cp bench/_job.sql airpulse-jobmanager:/tmp/bench.sql
T_SUBMIT=$(date +%s.%N)
JID=$(docker exec airpulse-jobmanager ./bin/sql-client.sh -f /tmp/bench.sql 2>/dev/null | sed -n 's/.*Job ID: \([0-9a-f]*\).*/\1/p' | tail -1)
[ -n "$JID" ] || { echo "submit failed"; exit 1; }
echo "   job $JID"

echo "== 4/5 timing: wait for the source to read all $N records"
SRC_ID=$(curl -s $REST/jobs/$JID | "$PY" -c "import sys,json; print(json.load(sys.stdin)['vertices'][0]['id'])")
T_FIRST=""; READ=0
while :; do
  READ=$(curl -s "$REST/jobs/$JID/vertices/$SRC_ID" | "$PY" -c "import sys,json; d=json.load(sys.stdin); print(sum(s['metrics']['write-records'] for s in d['subtasks']))")
  [ "$READ" -gt 0 ] && [ -z "$T_FIRST" ] && T_FIRST=$(date +%s.%N)
  [ "$READ" -ge "$N" ] && break
  sleep 0.5
done
T_DONE=$(date +%s.%N)
sleep 3                                       # let the JDBC sink flush its last buffer
ROWS=$($PSQL -c "select count(*) from bench_station_aqi_hourly")
AQI=$($PSQL -c "select count(aqi) from bench_station_aqi_hourly")

echo "== 5/5 cleanup"
curl -s -X PATCH "$REST/jobs/$JID?mode=cancel" >/dev/null
docker exec airpulse-redpanda rpk topic delete $TOPIC >/dev/null
rm -f bench/_job.sql

"$PY" - "$GEN" "$N" "$T_SUBMIT" "$T_FIRST" "$T_DONE" "$ROWS" "$AQI" "$PAR" <<'PY'
import json, sys, platform, datetime
gen, n, ts, tf, td, rows, aqi, par = sys.argv[1:]
gen = json.loads(gen); n = int(n); ts, tf, td = map(float, (ts, tf, td))
proc = td - tf; total = td - ts
r = {
  "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
  "machine": platform.platform(),
  "messages": n, "stations": gen["stations"], "hours": gen["hours"],
  "publish_msgs_per_sec": gen["msgs_per_sec"], "publish_mb_per_sec": gen["mb_per_sec"],
  "flink_startup_sec": round(tf - ts, 1),
  "flink_processing_sec": round(proc, 1),
  "flink_msgs_per_sec": round(n / proc),
  "flink_x_live_rate": round(n / proc / 0.2),
  "gold_rows": int(rows), "gold_rows_with_aqi": int(aqi),
  "flink_parallelism": int(par), "taskmanager_slots": 8,
}
import os
hist = json.load(open("bench/results.json")) if os.path.exists("bench/results.json") else []
hist = [h for h in hist if h.get("flink_parallelism") != int(par) or h.get("hours") != gen["hours"]] + [r]
json.dump(hist, open("bench/results.json", "w"), indent=2)
print(json.dumps(r, indent=2))
print("\n| messages | publish | Flink throughput | vs live feed | startup | gold rows |")
print("|---|---|---|---|---|---|")
print(f"| {n:,} ({gen['hours']} h x {gen['stations']} stations) | {gen['msgs_per_sec']:,}/s | "
      f"**{r['flink_msgs_per_sec']:,} msg/s** (parallelism {par}) | {r['flink_x_live_rate']:,}x | "
      f"{r['flink_startup_sec']} s | {int(rows):,} ({int(aqi):,} with AQI) |")
PY
