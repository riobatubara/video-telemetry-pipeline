import logging

from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)

from pyflink.common import Types

from job.config import config

logger = logging.getLogger("DLQ_Sink")

def create_dlq_sink():

    try:
        logger.info("creating ClickHouse JDBC DLQ sink")

        return JdbcSink.sink(
            """
            INSERT INTO invalid_event_log
            (
                tsserver,
                gateway_ip,
                uadev,
                error_reason,
                raw_payload
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            
            type_info=Types.ROW([
                Types.LONG(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.STRING()
            ]),

            jdbc_connection_options=JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .with_url(config.CLICKHOUSE_JDBC_URL)
                .with_driver_name("com.clickhouse.jdbc.ClickHouseDriver")
                .with_user_name(config.CLICKHOUSE_USER)
                .with_password(config.CLICKHOUSE_PASSWORD)
                .build(),

            jdbc_execution_options=JdbcExecutionOptions.builder()
                .with_batch_size(200)
                .with_batch_interval_ms(1000)
                .with_max_retries(3)
                .build()
        )
    
    except Exception:
        logger.exception("failed creating DLQ JDBC sink")
        raise