import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
    KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "video.telemetry.raw")
    KAFKA_TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ", "video.telemetry.dlq")
    KAFKA_GROUP_RAW = os.getenv("KAFKA_GROUP_RAW", "flink_raw_group")
    KAFKA_GROUP_DLQ = os.getenv("KAFKA_GROUP_DLQ", "flink_dlq_group")


    CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "default")
    CLICKHOUSE_JDBC_URL = (
        f"jdbc:clickhouse://{CLICKHOUSE_HOST}:"
        f"{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
    )

config = Config()