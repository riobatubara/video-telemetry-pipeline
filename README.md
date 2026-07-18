# video-telemetry-pipeline

#### Architecture
![Architecture Diagram](./architecture.png)

<!-- docker run --rm apache/kafka:3.7.0 /opt/kafka/bin/kafka-storage.sh random-uuid

docker compose --env-file .env up -d --build

docker exec -it telemetry_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic video.telemetry.raw

docker exec -it telemetry_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic video.telemetry.dlq

docker compose down stream-processor && docker compose up -d --build stream-processor
docker build -t stream-processor ./stream-processor --no-cache

docker exec -it telemetry_clickhouse clickhouse-client -q "SHOW TABLES"
output:
event_log
invalid_telemetry_log
video_measure

curl -X POST http://localhost:8080/api/v1/telemetry \
  -H "Content-Type: application/json" \
  -H "X-API-Key: apikeysecret" \
  -d 'This is completely broken garbage data text'
docker exec -it telemetry_clickhouse clickhouse-client -q "SELECT error_reason, raw_payload FROM invalid_event_log FORMAT PrettyCompact"


docker exec -it telemetry_clickhouse clickhouse-client -q "SELECT * FROM event_log FORMAT PrettyCompact"
output:
┌───────tsclient─┬─────────tsserver─┬─sessid───────────────────────────┬─event──────┬─value──────────────┐
│  1783682317183 │    1783682320000 │ uE5sXMWq0CCYL7IrsDeM4vqklbMnrsI8 │ video_name │ La La Land         │
│  1783682318587 │    1783682320002 │ uE5sXMWq0CCYL7IrsDeM4vqklbMnrsI8 │ playing    │ 1783682318587,5.00 │
└────────────────┴──────────────────┴──────────────────────────────────┴────────────┴────────────────────┘



docker build -t api-gateway -f api-gateway/Dockerfile
make run concurrent=1 api_url="http://localhost:8080/api/v1/telemetry" api_key="apikeysecret"

go run main.go concurrent=10 api_url="http://localhost:8080/api/v1/telemetry" api_key="apikeysecret" | grep -o "seek"

make run concurrent=1 api_url="http://localhost:8080/api/v1/telemetry" api_key="apikeysecret" | grep -o "seek" -->
