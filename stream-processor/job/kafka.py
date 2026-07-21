import json
import logging

from pyflink.common import Types, Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer
)
from pyflink.common.serialization import SimpleStringSchema

from pyflink.common.watermark_strategy import WatermarkStrategy
watermark_strategy=WatermarkStrategy.no_watermarks()

from job.config import config
from sinks.dlq import create_dlq_sink
from sinks.raw import create_raw_sink


logger = logging.getLogger("Kafka")


def create_kafka_source(topic: str, group_id: str) -> KafkaSource:
    try:
        logger.info("creating kafka source topic=%s group=%s", topic, group_id)

        return KafkaSource.builder() \
            .set_bootstrap_servers(config.KAFKA_BROKERS) \
            .set_topics(topic) \
            .set_group_id(group_id) \
            .set_starting_offsets(
                KafkaOffsetsInitializer.latest()
            ) \
            .set_value_only_deserializer(
                SimpleStringSchema()
            ) \
            .build()
    except Exception:
        logger.exception("failed creating kafka source topic=%s", topic)
        raise


def kafka_raw_source() -> KafkaSource:
    return create_kafka_source(
        topic=config.KAFKA_TOPIC_RAW,
        group_id=config.KAFKA_GROUP_RAW
    )


def kafka_dlq_source() -> KafkaSource:
    return create_kafka_source(
        topic=config.KAFKA_TOPIC_DLQ,
        group_id=config.KAFKA_GROUP_DLQ
    )


def process_dlq(message: str):
    try:
        payload = json.loads(message)

        event = payload.get("event", "")
        sessid = payload.get("sessid", "")
        value = payload.get("value", "")

        error_reason = event if event else sessid

        gateway_ip = "API_INTERNAL_GATEWAY"
        uadev = "DLQ_ERROR_AGENT"

        if event == "geoip":
            gateway_ip = value

        if event == "uadev":
            uadev = value

        logger.info("DLQ processed=%s", error_reason)

        return Row(
            payload.get("tsserver", 0),
            gateway_ip,
            uadev,
            error_reason,
            message
        )

    except Exception:
        logger.exception("DLQ parsing failed: %s", message)
        return None


def start_dlq_pipeline(env: StreamExecutionEnvironment):

    try:

        stream = env.from_source(
            kafka_dlq_source(),
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"kafka-source-{config.KAFKA_TOPIC_DLQ}"
        )

        stream \
            .map(
                process_dlq,
                output_type=Types.ROW([
                    Types.LONG(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
                ])
            ) \
            .filter(lambda x: x is not None) \
            .add_sink(create_dlq_sink())

    except Exception:
        logger.exception("failed starting DLQ processor")
        raise


def process_raw_message(message: str):
    try:
        payload = json.loads(message)

        return Row(
            payload.get("tsclient", 0),
            payload.get("tsserver", 0),
            payload.get("sessid", ""),
            payload.get("event", ""),
            payload.get("value", "")
        )

    except Exception:
        logger.exception("RAW parsing failed: %s", message)
        return None


def start_raw_pipeline(env: StreamExecutionEnvironment):

    try:

        stream = env.from_source(
            kafka_raw_source(),
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name=f"kafka-source-{config.KAFKA_TOPIC_RAW}"
        )

        stream \
            .map(
                process_raw_message,
                output_type=Types.ROW([
                    Types.LONG(),
                    Types.LONG(),
                    Types.STRING(),
                    Types.STRING(),
                    Types.STRING()
                ])
            ) \
            .filter(lambda x: x is not None) \
            .add_sink(create_raw_sink())

    except Exception:
        logger.exception("failed starting RAW processor")
        raise