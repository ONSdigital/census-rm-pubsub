import json
import logging
import os
import time

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from sdc.rabbit import QueuePublisher
from sdc.rabbit.exceptions import PublishMessageError
from structlog import wrap_logger

from app.app_logging import logger_initial_config


logger = wrap_logger(logging.getLogger(__name__))
subscriber = SubscriberClient()


GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
EQ_TOPIC_NAME = os.getenv("GCP_TOPIC_NAME", "eq-submission-topic")
RM_SUBSCRIPTION_NAME = os.getenv("GCP_SUBSCRIPTION_NAME", "rm-receipt-subscription")

RM_RABBIT_AMQP = os.getenv("RABBIT_AMQP")
RM_RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "Case.Responses")


def receipt_to_case(message: Message):
    assert message.attributes['eventType'] == 'OBJECT_FINALIZE'  # receipt only on object creation
    bucket_name, object_name = message.attributes['bucketId'], message.attributes['objectId']
    logger.debug('Message received for processing', bucket_name=bucket_name,
                                                    message_id=message.message_id,
                                                    object_name=object_name)
    _ = json.dumps(str(message.data))  # TODO: maybe find case_id here?
    # Expects message to contain (at least) case_id AND inbound_channel (OFFLINE, ONLINE, PAPER)
    # TODO: This assumes caseId is filename
    send_message_to_rabbitmq(message={'inboundChannel': 'ONLINE', 'caseId': object_name})
    message.ack()


def setup_subscription(subscription_path, topic_path):
    subscriber.create_subscription(subscription_path, topic_path)
    subscriber_future = subscriber.subscribe(subscription_path, receipt_to_case)
    logger.info('Listening for messages', subscription_path=subscription_path, topic_path=topic_path)
    return subscriber_future


def init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE):
    logger.debug('Connecting to rabbitmq', url=rabbitmq_amqp)
    publisher = QueuePublisher([rabbitmq_amqp], queue_name)
    publisher._connect()  # NOQA
    logger.info('Successfully initialised rabbitmq', queue=queue_name)


def send_message_to_rabbitmq(message, rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE):
    """
    Send message to rabbitmq
    :param message: The message to send to the queue in JSON format
    :param queue_name: The rabbit queue or exchange to publish to
    :return: boolean
    """
    logger.debug('Connecting to rabbitmq', url=rabbitmq_amqp)
    publisher = QueuePublisher([rabbitmq_amqp], queue_name)
    try:
        result = publisher.publish_message(message, immediate=False, mandatory=True)
        logger.info('Message successfully sent to rabbitmq', queue=queue_name)
        return result
    except PublishMessageError:
        logger.exception('Message failed to send to rabbitmq', queue=queue_name)
    return False


def main():
    logger_initial_config(service_name="census-rm-pubsub", log_level=os.getenv("LOG_LEVEL"))
    topic_path = f'projects/{GCP_PROJECT_ID}/topics/{EQ_TOPIC_NAME}'
    subscription_path = f'projects/{GCP_PROJECT_ID}/subscriptions/{RM_SUBSCRIPTION_NAME}'

    init_rabbitmq()

    subscriber_future = setup_subscription(subscription_path, topic_path)
    while True:  # setup_subscription creates a background thread for processing messages
        try:
            subscriber_future.result(timeout=None)  # block main thread while polling for messages indefinitely
        except:
            logger.exception('Error processing message')
        time.sleep(30)


if __name__ == '__main__':
    main()