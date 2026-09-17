#!/usr/bin/env sh
# Fetch the connector JARs the Flink image needs. Pinned to Flink 1.20.
# Run once after cloning:  sh flink/download-jars.sh
set -e
cd "$(dirname "$0")/lib"
M=https://repo1.maven.org/maven2
for u in \
  "$M/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
  "$M/org/apache/flink/flink-connector-jdbc-core/3.4.0-1.20/flink-connector-jdbc-core-3.4.0-1.20.jar" \
  "$M/org/apache/flink/flink-connector-jdbc-postgres/3.4.0-1.20/flink-connector-jdbc-postgres-3.4.0-1.20.jar" \
  "$M/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar"; do
  f=$(basename "$u"); [ -f "$f" ] || { echo "fetching $f"; curl -sSL -o "$f" "$u"; }
done
echo "jars ready"
