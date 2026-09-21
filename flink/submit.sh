#!/usr/bin/env bash
# Submit every flink/sql/*.sql job to the cluster, once. Idempotent: a job whose
# sink name is already RUNNING is skipped, so `docker compose up` after a
# restart does not create duplicates.
set -euo pipefail
REST=${FLINK_REST:-http://flink-jobmanager:8081}
# This container bypasses Flink's entrypoint, so point the SQL client at the
# cluster ourselves (Flink 1.20 reads conf/config.yaml).
grep -q '^rest.address:' /opt/flink/conf/config.yaml || printf 'rest.address: flink-jobmanager
jobmanager.rpc.address: flink-jobmanager
' >> /opt/flink/conf/config.yaml

echo "waiting for Flink REST at $REST"
until curl -sf "$REST/overview" >/dev/null; do sleep 3; done
# Wait for at least one TaskManager slot, or the job would be SCHEDULED forever.
until [ "$(curl -sf "$REST/overview" | sed -n 's/.*"slots-total":\([0-9]*\).*/\1/p')" -gt 0 ]; do sleep 3; done

running=$(curl -sf "$REST/jobs/overview" | tr ',' '\n' | sed -n 's/.*"name":"\([^"]*\)".*/\1/p; s/.*"state":"\([^"]*\)".*/\1/p' | paste - - | grep -vE 'CANCELED|FAILED|FINISHED' || true)

for f in /opt/flink/sql/*.sql; do
  # The sink table names in the file are what Flink names the jobs after.
  sinks=$(sed -n 's/^INSERT INTO \([a-z_0-9]*\).*/\1/p' "$f")
  skip=1
  for s in $sinks; do echo "$running" | grep -q "$s" || skip=0; done
  if [ "$skip" = 1 ]; then echo "skip $(basename "$f"): already running"; continue; fi
  echo "submitting $(basename "$f")"
  /opt/flink/bin/sql-client.sh -f "$f" 2>&1 | grep -E "Job ID|ERROR|Exception" || true
done
echo "done"
