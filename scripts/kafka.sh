#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
compose=(docker compose -f infrastructure/kafka/docker-compose.yml)
kafka() { "${compose[@]}" exec -T -e 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m' kafka "/opt/kafka/bin/$1" "${@:2}"; }
case "${1:-help}" in
  start) "${compose[@]}" up -d --wait --wait-timeout 180 ;;
  topic)
    kafka kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists --topic banking.transactions.raw --partitions 3 --replication-factor 1 --config retention.ms=86400000 --config retention.bytes=268435456
    kafka kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic banking.transactions.raw
    ;;
  smoke|persistence)
    topic="banking.smoke.$(date +%s).$$"
    cleanup() { kafka kafka-topics.sh --bootstrap-server kafka:29092 --delete --topic "$topic" >/dev/null; }
    kafka kafka-topics.sh --bootstrap-server kafka:29092 --create --topic "$topic" --partitions 1 --replication-factor 1 >/dev/null
    trap cleanup EXIT
    payload="{\"test\":\"$topic\"}"
    printf '%s\n' "$payload" | kafka kafka-console-producer.sh --bootstrap-server kafka:29092 --topic "$topic"
    if [ "$1" = persistence ]; then
      "${compose[@]}" stop kafka
      "${compose[@]}" up -d --wait --wait-timeout 180
    fi
    received=$(kafka kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic "$topic" --from-beginning --max-messages 1 --timeout-ms 15000)
    test "$received" = "$payload"
    echo 'PASS: Kafka a restitué le message envoyé.'
    ;;
  status) "${compose[@]}" ps ;;
  logs) "${compose[@]}" logs --tail 80 kafka ;;
  stop) "${compose[@]}" stop ;;
  *) echo 'Usage: bash scripts/kafka.sh {start|topic|smoke|persistence|status|logs|stop}'; exit 2 ;;
esac
