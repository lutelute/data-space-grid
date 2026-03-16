#!/usr/bin/env bash
# topics.sh
#
# Initialize Kafka topics for the Federated Data Space event bus.
#
# Topics:
#   dr-events          - DR event notifications (DSO -> Aggregator, Prosumer)
#   dispatch-commands  - Real-time dispatch commands (DSO -> Aggregator)
#   dispatch-actuals   - Dispatch response actuals (Aggregator -> DSO)
#   congestion-alerts  - Real-time congestion level changes (DSO -> Aggregator)
#   audit-events       - Audit entries for centralized analysis (All nodes -> Audit)
#
# Usage:
#   bash infrastructure/kafka/topics.sh
#
# Prerequisites:
#   Kafka must be running. Default bootstrap server: localhost:9092
#   When running inside Docker Compose, the script executes via the Kafka container.

set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
PARTITIONS="${KAFKA_DEFAULT_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"

# Detect whether we're running inside the Kafka container or externally.
# Inside Docker Compose, kafka-topics.sh is on PATH; outside, use docker exec.
if command -v kafka-topics.sh &>/dev/null; then
    KAFKA_TOPICS_CMD="kafka-topics.sh"
elif command -v kafka-topics &>/dev/null; then
    KAFKA_TOPICS_CMD="kafka-topics"
else
    echo "kafka-topics.sh not found on PATH."
    echo "Attempting to run via docker compose exec..."
    KAFKA_TOPICS_CMD="docker compose exec -T kafka kafka-topics.sh"
fi

create_topic() {
    local topic_name="$1"
    local partitions="$2"
    local retention_ms="$3"
    local description="$4"

    echo "Creating topic: ${topic_name} (partitions=${partitions}, retention=${retention_ms}ms)"
    ${KAFKA_TOPICS_CMD} \
        --bootstrap-server "${BOOTSTRAP_SERVER}" \
        --create \
        --if-not-exists \
        --topic "${topic_name}" \
        --partitions "${partitions}" \
        --replication-factor "${REPLICATION_FACTOR}" \
        --config retention.ms="${retention_ms}" \
        --config cleanup.policy=delete
    echo "  -> ${description}"
}

echo "============================================"
echo "Federated Data Space - Kafka Topic Setup"
echo "============================================"
echo ""
echo "Bootstrap server: ${BOOTSTRAP_SERVER}"
echo "Default partitions: ${PARTITIONS}"
echo "Replication factor: ${REPLICATION_FACTOR}"
echo ""

# DR event notifications: DSO publishes, Aggregator and Prosumer consume.
# Retention: 7 days (604800000 ms) - events are time-bounded.
create_topic "dr-events" "${PARTITIONS}" "604800000" \
    "DR event notifications (OpenADR-style). Producer: DSO. Consumers: Aggregator, Prosumer."

# Dispatch commands: DSO sends real-time dispatch instructions to Aggregator.
# Retention: 1 day (86400000 ms) - commands are ephemeral.
create_topic "dispatch-commands" "${PARTITIONS}" "86400000" \
    "Real-time dispatch commands. Producer: DSO. Consumer: Aggregator."

# Dispatch actuals: Aggregator reports actual dispatch results back to DSO.
# Retention: 30 days (2592000000 ms) - actuals need longer retention for reconciliation.
create_topic "dispatch-actuals" "${PARTITIONS}" "2592000000" \
    "Dispatch response actuals. Producer: Aggregator. Consumer: DSO."

# Congestion alerts: DSO publishes real-time congestion level changes.
# Retention: 1 day (86400000 ms) - alerts are time-critical and ephemeral.
create_topic "congestion-alerts" "${PARTITIONS}" "86400000" \
    "Real-time congestion level changes. Producer: DSO. Consumer: Aggregator."

# Audit events: All participant nodes publish audit entries for centralized analysis.
# Retention: 90 days (7776000000 ms) - audit trail requires long retention.
create_topic "audit-events" "${PARTITIONS}" "7776000000" \
    "Audit entries for centralized analysis. Producers: All nodes. Consumer: Audit service."

echo ""
echo "============================================"
echo "Topic initialization complete."
echo "============================================"
echo ""

echo "Listing all topics:"
${KAFKA_TOPICS_CMD} \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --list
