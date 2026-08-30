#!/usr/bin/env bash
# Creates the six OMS topics from the architecture diagram.
# Run once after `docker-compose up -d`.
# Container name may be sda-kafka-1 or kafka — check with `docker ps`.
CONTAINER=${1:-sda-kafka-1}

for TOPIC in orders-topic payments-topic fulfillment-topic \
             inventory-topic returns-topic crm-events-topic
do
  docker exec -it $CONTAINER kafka-topics.sh --create \
    --topic $TOPIC \
    --bootstrap-server localhost:9092 \
    --replication-factor 1 \
    --partitions 3
done

docker exec -it $CONTAINER kafka-topics.sh --list --bootstrap-server localhost:9092
