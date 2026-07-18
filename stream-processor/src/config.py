import os

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ")

GROUP_RAW = os.getenv("KAFKA_GROUP_RAW", "flink-raw-group") + "_clean_flat_v65"
GROUP_DLQ = os.getenv("KAFKA_GROUP_DLQ", "flink-dlq-group") + "_clean_flat_v65"

CLICKHOUSE_URL = f"clickhouse://{os.getenv('CLICKHOUSE_HOST')}:{os.getenv('CLICKHOUSE_PORT')}"
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

def print_metrics():
    print("PIPELINE METRICS:")
    print(f" Kafka Broker: {KAFKA_BROKERS} | Topic: {TOPIC_RAW}, {TOPIC_DLQ}")
    print(f" ClickHouse Node: {CLICKHOUSE_URL}/{CLICKHOUSE_DB}")
    print("-"*60)
