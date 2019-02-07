import logging
import os

import pika
from structlog import wrap_logger


logger = wrap_logger(logging.getLogger(__name__))

RM_RABBIT_AMQP = os.getenv("RABBIT_AMQP", "amqp://guest:guest@localhost:6672")
RM_RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "case-outbound-exchange")
RM_RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "Case.Responses.binding")


def init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE):
    """
    Initialise connection to rabbitmq

    :param rabbitmq_amqp: The amqp (url) of the rabbit queue
    :param queue_name: The rabbit queue to publish to
    """
    logger.debug('Connecting to rabbitmq', url=rabbitmq_amqp)
    rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_amqp))
    _ = rabbitmq_connection.channel()
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
    rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_amqp))
    rabbitmq_channel = rabbitmq_connection.channel()
    rabbitmq_channel.basic_publish(exchange=RM_RABBIT_EXCHANGE,
                                   routing_key=queue_name,
                                   body=str(message),
                                   properties=pika.BasicProperties(content_type='text/xml'))
    logger.info('Message successfully sent to rabbitmq', queue=queue_name)
    rabbitmq_connection.close()
