import logging
import os
import time

from structlog import wrap_logger

from app.app_logging import logger_initial_config
from app.rabbit_helper import init_rabbitmq
from app.subscriber import setup_subscription


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
EQ_TOPIC_NAME = os.getenv("GCP_TOPIC_NAME", "eq-submission-topic")
RM_SUBSCRIPTION_NAME = os.getenv("GCP_SUBSCRIPTION_NAME", "rm-receipt-subscription")

logger = wrap_logger(logging.getLogger(__name__))


def main():
    """
    Main entry point of the subscriber worker
    """
    logger_initial_config(service_name="census-rm-pubsub", log_level=os.getenv("LOG_LEVEL"))

    init_rabbitmq()

    subscriber_future = setup_subscription(GCP_PROJECT_ID, RM_SUBSCRIPTION_NAME, EQ_TOPIC_NAME)
    while True:  # setup_subscription creates a background thread for processing messages
        subscriber_future.result(timeout=None)  # block main thread while polling for messages indefinitely
        time.sleep(30)


if __name__ == '__main__':
    main()
