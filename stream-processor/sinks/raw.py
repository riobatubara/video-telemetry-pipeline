import logging

from pyflink.datastream.connectors.jdbc import (
    JdbcSink,
    JdbcConnectionOptions,
    JdbcExecutionOptions,
)

from pyflink.common import Types

from job.config import config

logger = logging.getLogger("RAW_Sink")

def create_raw_sink():

    try:
        logger.info("creating ClickHouse JDBC raw sink")

        return JdbcSink.sink(
            """
            INSERT INTO event_log
            (
                tsclient,
                tsserver,
                sessid,
                event,
                value
            )
            VALUES (?, ?, ?, ?, ?)
            """,

            type_info=Types.ROW([
                Types.LONG(),
                Types.LONG(),
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
        logger.exception("failed creating raw JDBC sink")
        raise
