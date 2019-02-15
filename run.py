import logging
import os
import time

from structlog import wrap_logger

from app.app_logging import logger_initial_config
from app.readiness import Readiness
from app.rabbit_helper import init_rabbitmq
from app.subscriber import setup_subscription


logger = wrap_logger(logging.getLogger(__name__))


def main():
    """
    Main entry point of the subscriber worker
    """
    logger_initial_config(service_name="census-rm-pubsub", log_level=os.getenv("LOG_LEVEL", "INFO"))

    init_rabbitmq()  # test the connection to the rabbitmq cluster

    subscriber_future = setup_subscription()
    with Readiness(os.getenv('READINESS_FILE_PATH')):  # Indicate ready after successful setup
        while True:  # setup_subscription creates a background thread for processing messages
            subscriber_future.result(timeout=None)  # block main thread while polling for messages indefinitely
            time.sleep(30)


if __name__ == '__main__':
    main()
