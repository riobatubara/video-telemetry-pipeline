import os
import sys

# ------------------------------------------------------------------------
# 1. PATH BINDING (Ensures background threads can find the Python execution loop)
# ------------------------------------------------------------------------
current_python_executable = sys.executable
os.environ["PYFLINK_CLIENT_EXECUTABLE"] = current_python_executable
os.environ["PYFLINK_SIGNAL_EXECUTABLE"] = current_python_executable

print("="*60)
print(f"🚀 EXECUTING CLEAN FLAT LOG PIPELINE -> {current_python_executable}")
print("="*60)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# ------------------------------------------------------------------------
# 2. LOAD ENVIRONMENT VARIABLES
# ------------------------------------------------------------------------
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW")
TOPIC_DLQ = os.getenv("KAFKA_TOPIC_DLQ")

# Increment the group ID string to bypass old checkpoint consumer positions
GROUP_RAW = os.getenv("KAFKA_GROUP_RAW", "flink-raw-group") + "_clean_flat_v65"
GROUP_DLQ = os.getenv("KAFKA_GROUP_DLQ", "flink-dlq-group") + "_clean_flat_v65"

CLICKHOUSE_URL = f"clickhouse://{os.getenv('CLICKHOUSE_HOST')}:{os.getenv('CLICKHOUSE_PORT')}"
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB')
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

print("--- PIPELINE METRICS ---")
print(f" Target Kafka Broker: {KAFKA_BROKERS} | Topic: {TOPIC_RAW}")
print(f" Target ClickHouse Node: {CLICKHOUSE_URL}/{CLICKHOUSE_DB}")
print("-"*60)

# ------------------------------------------------------------------------
# 3. ENVIRONMENT STANDUP
# ------------------------------------------------------------------------
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Checkpoint every 5 seconds to give the consumer breathing room
env.enable_checkpointing(5000) 

settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
t_env = StreamTableEnvironment.create(env, environment_settings=settings)

# ------------------------------------------------------------------------
# 4. DATA TABLES DEFINITIONS
# ------------------------------------------------------------------------
t_env.execute_sql(f"""
CREATE TABLE kafka_raw (
    tsclient BIGINT,
    tsserver BIGINT,
    sessid STRING,
    event STRING,
    `value` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{TOPIC_RAW}',
    'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
    'properties.group.id' = '{GROUP_RAW}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")

t_env.execute_sql(f"""
CREATE TABLE kafka_dlq (
    tsclient BIGINT,
    tsserver BIGINT,
    sessid STRING,
    event STRING,
    `value` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{TOPIC_DLQ}',
    'properties.bootstrap.servers' = '{KAFKA_BROKERS}',
    'properties.group.id' = '{GROUP_DLQ}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
)
""")

# ClickHouse Sinks
t_env.execute_sql(f"""
CREATE TABLE ch_event_log (
    tsclient BIGINT,
    tsserver BIGINT,
    sessid STRING,
    event STRING,
    `value` STRING
) WITH (
    'connector' = 'clickhouse',
    'url' = '{CLICKHOUSE_URL}',
    'database-name' = '{CLICKHOUSE_DB}',
    'table-name' = 'event_log',
    'username' = '{CLICKHOUSE_USER}',
    'password' = '{CLICKHOUSE_PASSWORD}',
    'sink.batch-size' = '1',
    'sink.flush-interval' = '1s'
)
""")

t_env.execute_sql(f"""
CREATE TABLE ch_invalid_log (
    tsserver BIGINT,
    gateway_ip STRING,
    uadev STRING,
    error_reason STRING,
    raw_payload STRING
) WITH (
    'connector' = 'clickhouse',
    'url' = '{CLICKHOUSE_URL}',
    'database-name' = '{CLICKHOUSE_DB}',
    'table-name' = 'invalid_event_log',
    'username' = '{CLICKHOUSE_USER}',
    'password' = '{CLICKHOUSE_PASSWORD}',
    'sink.batch-size' = '1',
    'sink.flush-interval' = '1s'
)
""")

# ------------------------------------------------------------------------
# 5. STREAM PIPELINE EXECUTION
# ------------------------------------------------------------------------
print("\n🔥 Activating direct Kafka-to-ClickHouse stream...")

statement_set = t_env.create_statement_set()

# Pipeline 1: Raw Stream Route
# table_result = t_env.execute_sql("""
# INSERT INTO ch_event_log
# SELECT
#     COALESCE(tsclient, 0),
#     COALESCE(tsserver, 0),
#     COALESCE(sessid, 'null'),
#     COALESCE(event, 'unknown_event'),
#     COALESCE(`value`, '')
# FROM kafka_raw
# """)
statement_set.add_insert_sql("""
INSERT INTO ch_event_log
SELECT
    COALESCE(tsclient, 0),
    COALESCE(tsserver, 0),
    COALESCE(sessid, 'null'),
    COALESCE(event, 'unknown_event'),
    COALESCE(`value`, '')
FROM kafka_raw
""")

# Pipeline 2: DLQ Route (Added safely alongside it)
statement_set.add_insert_sql("""
INSERT INTO ch_invalid_log
SELECT 
    COALESCE(tsserver, 0),
    'API_INTERNAL_GATEWAY' AS gateway_ip,
    'DLQ_ERROR_AGENT' AS uadev,
    COALESCE(event, 'MALFORMED') AS error_reason,
    COALESCE(`value`, '') AS raw_payload
FROM kafka_dlq
""")

# Submit and block container thread active
job_result = statement_set.execute()
print("Job submitted successfully! Locking container thread active...")
job_result.wait()

# CRITICAL FIX: Blocks the python process from exiting so the stream stays alive
# table_result.wait()
