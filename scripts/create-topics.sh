#!/usr/bin/env sh
set -eu
[ -f .env ] && set -a && . ./.env && set +a

for topic in "${KAFKA_TOPIC_SERVER_LOGS:-server-logs}" "${KAFKA_TOPIC_ANOMALIES:-anomalies}"; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done
