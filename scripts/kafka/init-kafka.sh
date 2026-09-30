#!/usr/bin/env bash
set -euo pipefail

# Block until Kafka is ready
echo "Waiting for ${KAFKA_EXPECTED_BROKERS:-3} Kafka brokers..."
cub kafka-ready -b "$KAFKA_BOOTSTRAP_SERVERS" "${KAFKA_EXPECTED_BROKERS:-3}" 60

# Array of topics to create
TOPICS=(
  "stock-trades:$TOPIC_PARTITIONS:$TOPIC_REPLICATION_FACTOR"
  "stock-features-raw:$TOPIC_PARTITIONS:$TOPIC_REPLICATION_FACTOR"
  "model-predictions:$TOPIC_PARTITIONS:$TOPIC_REPLICATION_FACTOR"
)

for topic in "${TOPICS[@]}"; do
  IFS=":" read -r name partitions rf <<< "$topic"
  
  echo "Checking/Creating topic: $name"
  # REMOVED the .sh extension
  kafka-topics --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor "$rf" \
    --config min.insync.replicas=2
done

echo "Kafka initialization complete. Mission is GO."
