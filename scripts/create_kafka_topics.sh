#!/bin/bash

KAFKA_HOST=${KAFKA_HOST:-localhost:9092}

# Function to create topic
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3
    
    kafka-topics --create \
        --bootstrap-server $KAFKA_HOST \
        --topic $topic_name \
        --partitions $partitions \
        --replication-factor $replication_factor \
        --config retention.ms=604800000 \  # 7 days retention
        --config compression.type=snappy
}

# Create our topics
create_topic "steam.players" 3 1
create_topic "steam.games" 3 1
create_topic "steam.stats" 3 1