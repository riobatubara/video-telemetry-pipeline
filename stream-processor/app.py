import os
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_telemetry_processor():
    # Fetch Authoritative Settings
    kafka_brokers = os.getenv("KAFKA_BROKERS", "kafka:9092")
    topic_raw = os.getenv("KAFKA_TOPIC_RAW", "video.telemetry.raw")
    topic_dlq = os.getenv("KAFKA_TOPIC_DLQ", "video.telemetry.dlq")

    group_raw = os.getenv("KAFKA_GROUP_RAW", "flink_telemetry_ingest_group")
    group_dlq = os.getenv("KAFKA_GROUP_DLQ", "flink_telemetry_dlq_group")
    
    ch_host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    ch_port = os.getenv("CLICKHOUSE_PORT", "9000")
    ch_db = os.getenv("CLICKHOUSE_DB", "default")
    ch_user = os.getenv("CLICKHOUSE_USER", "default")
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "")

    # Start Stream Processing Engine Environments
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    # Add Necessary Kafka Connector JAR Dependencies dynamically
    # This allows Flink to translate Kafka data structures natively
    t_env.get_config().set(
        "pipeline.jars",
        "flink-sql-connector-kafka-3.1.0-1.18.jar;flink-connector-jdbc-3.1.1-1.18.jar"
    )

    # Define Inbound Kafka Telemetry Source Table
    # This reads the string payload arrays passed down from your API-Gateway
    t_env.execute_sql(f"""
        CREATE TABLE kafka_raw_stream (
            tsclient BIGINT,
            sessid STRING,
            event STRING,
            value STRING,
            tsserver BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic_raw}',
            'properties.bootstrap.servers' = '{kafka_brokers}',
            'properties.group.id' = 'flink_telemetry_ingest_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # Define our Inbound Kafka DLQ(Dead Letter Queue) Source Table
    t_env.execute_sql(f"""
        CREATE TABLE kafka_dlq_stream (
            tsserver BIGINT,
            gateway_ip STRING,
            uadev STRING,
            error_reason STRING,
            raw_payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic_dlq}',
            'properties.bootstrap.servers' = '{kafka_brokers}',
            'properties.group.id' = 'flink_telemetry_dlq_group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json'
        )
    """)

    # ClickHouse Event Log Sink Target
    t_env.execute_sql(f"""
        CREATE TABLE event_log_sink (
            tsclient BIGINT,
            tsserver BIGINT,
            sessid STRING,
            event STRING,
            value STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}',
            'table-name' = 'event_log',
            'username' = '{ch_user}',
            'password' = '{ch_password}'
        )
    """)

    # Clickhouse Invalid Event Log Sink Target
    t_env.execute_sql(f"""
        CREATE TABLE invalid_event_log_sink (
            tsserver BIGINT,
            gateway_ip STRING,
            uadev STRING,
            error_reason STRING,
            raw_payload STRING
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:clickhouse://{ch_host}:{ch_port}/{ch_db}',
            'table-name' = 'invalid_telemetry_log',
            'username' = '{ch_user}',
            'password' = '{ch_password}'
        )
    """)

    # Executing pipeline
    statement_set = t_env.create_statement_set()

    # Push raw stream from Kafka directly into ClickHouse Schema event_log
    statement_set.add_insert_sql("""
        INSERT INTO event_log_sink
        SELECT tsclient, tsserver, sessid, event, value 
        FROM kafka_raw_stream
    """)

    # Push broken stream from Kafka directly into ClickHouse Schema invalid_telemetry_log
    statement_set.add_insert_sql("""
        INSERT INTO invalid_event_log_sink
        SELECT tsserver, gateway_ip, uadev, error_reason, raw_payload
        FROM kafka_dlq_stream
    """)

    # Execute the streaming jobs simultaneously
    print("PyFlink streaming topologies compiled successfully. Starting job clusters...")
    statement_set.execute()

if __name__ == '__main__':
    run_telemetry_processor()
