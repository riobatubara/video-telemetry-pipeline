from src import config

def register_tables(t_env):
    # 1. Kafka Raw Source
    t_env.execute_sql(f"""
    CREATE TABLE kafka_raw (
        tsclient BIGINT,
        tsserver BIGINT,
        sessid STRING,
        event STRING,
        `value` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{config.TOPIC_RAW}',
        'properties.bootstrap.servers' = '{config.KAFKA_BROKERS}',
        'properties.group.id' = '{config.GROUP_RAW}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """)

    # 2. Kafka DLQ Source
    t_env.execute_sql(f"""
    CREATE TABLE kafka_dlq (
        tsclient BIGINT,
        tsserver BIGINT,
        sessid STRING,
        event STRING,
        `value` STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{config.TOPIC_DLQ}',
        'properties.bootstrap.servers' = '{config.KAFKA_BROKERS}',
        'properties.group.id' = '{config.GROUP_DLQ}',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'false',
        'json.ignore-parse-errors' = 'true'
    )
    """)

    # 3. ClickHouse Main Sink
    t_env.execute_sql(f"""
    CREATE TABLE ch_event_log (
        tsclient BIGINT,
        tsserver BIGINT,
        sessid STRING,
        event STRING,
        `value` STRING
    ) WITH (
        'connector' = 'clickhouse',
        'url' = '{config.CLICKHOUSE_URL}',
        'database-name' = '{config.CLICKHOUSE_DB}',
        'table-name' = 'event_log',
        'username' = '{config.CLICKHOUSE_USER}',
        'password' = '{config.CLICKHOUSE_PASSWORD}',
        'sink.batch-size' = '1',
        'sink.flush-interval' = '1s'
    )
    """)

    # 4. ClickHouse Invalid Sink
    t_env.execute_sql(f"""
    CREATE TABLE ch_invalid_log (
        tsserver BIGINT,
        gateway_ip STRING,
        uadev STRING,
        error_reason STRING,
        raw_payload STRING
    ) WITH (
        'connector' = 'clickhouse',
        'url' = '{config.CLICKHOUSE_URL}',
        'database-name' = '{config.CLICKHOUSE_DB}',
        'table-name' = 'invalid_event_log',
        'username' = '{config.CLICKHOUSE_USER}',
        'password' = '{config.CLICKHOUSE_PASSWORD}',
        'sink.batch-size' = '1',
        'sink.flush-interval' = '1s'
    )
    """)
