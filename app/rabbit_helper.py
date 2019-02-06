import logging
import os

from sdc.rabbit import QueuePublisher
from sdc.rabbit.exceptions import PublishMessageError
from structlog import wrap_logger

logger = wrap_logger(logging.getLogger(__name__))


RM_RABBIT_AMQP = os.getenv("RABBIT_AMQP")
RM_RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "Case.Responses")


def init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE):
    """
    Initialise connection to rabbitmq

    :param rabbitmq_amqp: The amqp (url) of the rabbit queue
    :param queue_name: The rabbit queue to publish to
    """
    logger.debug('Connecting to rabbitmq', url=rabbitmq_amqp)
    publisher = QueuePublisher([rabbitmq_amqp], queue_name)
    publisher._connect()  # NOQA
    logger.info('Successfully initialised rabbitmq', queue=queue_name)


def send_message_to_rabbitmq(message, rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE):
    """
    Send message to rabbitmq

    :param message: The message to send to the queue in JSON format
    :param rabbitmq_amqp: The amqp (url) of the rabbit queue
    :param queue_name: The rabbit queue to publish to
    :return: boolean
    :raises: PublishMessageError
    """
    logger.debug('Connecting to rabbitmq', url=rabbitmq_amqp)
    publisher = QueuePublisher([rabbitmq_amqp], queue_name)
    try:
        publisher.publish_message(message, immediate=False, mandatory=True)
        logger.info('Message successfully sent to rabbitmq', queue=queue_name)
    except PublishMessageError:
        logger.exception('Message failed to send to rabbitmq', queue=queue_name)
        raise
