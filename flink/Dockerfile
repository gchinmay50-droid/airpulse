# Official Flink image + the connector JARs we need on the classpath.
# Kafka source, JDBC sink (Postgres dialect + driver).
FROM flink:1.20.5-scala_2.12-java17
COPY lib/*.jar /opt/flink/lib/
