import logging

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import RestartStrategies

from job.kafka import (
    start_dlq_pipeline,
    start_raw_pipeline
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s"
)

logger = logging.getLogger("stream-processor")


def main():

    try:
        env = StreamExecutionEnvironment.get_execution_environment()

        env.set_parallelism(1)

        env.set_restart_strategy(
            RestartStrategies.fixed_delay_restart(
                3,
                10000
            )
        )

        start_dlq_pipeline(env)
        start_raw_pipeline(env)

        logger.info("executing job")
        # logger.info(
        #     "Submitting Flink job %s",
        #     config.FLINK_JOB_NAME
        # )

        env.execute("video-telemetry-stream-processor")

    except Exception:
        logger.exception("Telemetry pipeline failed")
        raise


if __name__ == "__main__":
    main()