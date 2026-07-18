def build_and_run_pipeline(t_env):
    statement_set = t_env.create_statement_set()

    # Route 1: Raw Stream
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

    # Route 2: DLQ Stream
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

    print("Job submitted successfully! Locking container thread active...")
    job_result = statement_set.execute()
    job_result.wait()
