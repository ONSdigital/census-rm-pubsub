import logging
import os
import threading
import time

from structlog import wrap_logger

from app.app_logging import logger_initial_config
from app.readiness import Readiness
from app.rabbit_helper import init_rabbitmq
from app.subscriber import setup_subscription, OFFLINE_SUBSCRIPTION_NAME, offline_receipt_to_case, \
    OFFLINE_SUBSCRIPTION_PROJECT_ID

logger = wrap_logger(logging.getLogger(__name__))


def main():
    """
    Main entry point of the subscriber worker
    """
    logger_initial_config(service_name="census-rm-pubsub", log_level=os.getenv("LOG_LEVEL", "INFO"))

    init_rabbitmq()  # test the connection to the rabbitmq cluster

    futures = [setup_subscription(),
               setup_subscription(subscription_name=OFFLINE_SUBSCRIPTION_NAME, callback=offline_receipt_to_case,
                                  subscription_project_id=OFFLINE_SUBSCRIPTION_PROJECT_ID)]

    with Readiness(os.getenv('READINESS_FILE_PATH',
                             os.path.join(os.getcwd(), 'pubsub-ready'))):  # Indicate ready after successful setup
        for subscriber_future in futures:
            thread = threading.Thread(target=subscriber_future.result, kwargs={"timeout": None}, daemon=True)
            thread.start()
        while True:  # setup_subscription creates a background thread for processing messages
            time.sleep(30)


if __name__ == '__main__':
    main()
