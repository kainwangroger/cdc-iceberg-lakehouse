#!/bin/bash
# Enregistre le connector Debezium PostgreSQL auprès de Kafka Connect
# Usage: bash register-postgres-connector.sh

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "Registering Debezium PostgreSQL connector..."

curl -s -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-cdc-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres-source",
      "database.port": "5432",
      "database.user": "postgres",
      "database.password": "postgres",
      "database.dbname": "source_db",
      "plugin.name": "pgoutput",
      "publication.name": "cdc_pub",
      "slot.name": "debezium_slot",
      "table.include.list": "sales.customers,sales.orders,sales.order_items,sales.inventory",
      "topic.prefix": "cdc",
      "decimal.handling.mode": "double",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter",
      "key.converter.schemas.enable": "false",
      "value.converter.schemas.enable": "false",
      "snapshot.mode": "initial",
      "heartbeat.interval.ms": "5000",
      "tombstones.on.delete": "false"
    }
  }'

echo ""
echo "Checking connector status..."
curl -s "${CONNECT_URL}/connectors/postgres-cdc-connector/status"
