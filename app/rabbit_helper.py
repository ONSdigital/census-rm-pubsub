import logging
import os
import pika
from structlog import wrap_logger


RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "case-outbound-exchange")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "Case.Responses")
RABBIT_ROUTE = os.getenv("RABBIT_ROUTING_KEY", "Case.Responses.binding")
RABBIT_QUEUE_ARGS = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                     'x-dead-letter-routing-key': RABBIT_ROUTE}

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_PORT = os.getenv("RABBIT_PORT", "6672")
RABBIT_VIRTUALHOST = os.getenv("RABBIT_VIRTUALHOST", "/")
RABBIT_USERNAME = os.getenv("RABBIT_USERNAME", "guest")
RABBIT_PASSWORD = os.getenv("RABBIT_PASSWORD", "guest")

logger = wrap_logger(logging.getLogger(__name__))


def init_rabbitmq(binding_key=RABBIT_ROUTE,
                  exchange_name=RABBIT_EXCHANGE,
                  queue_name=RABBIT_QUEUE,
                  queue_args=RABBIT_QUEUE_ARGS):
    """
    Initialise connection to rabbitmq

    :param parameters: The CollectionParameters for the connection
    :param exchange_name: The rabbitmq exchange to publish to, (e.g.: "case-outbound-exchange")
    :param queue_name: The rabbitmq queue that subscribes to the exchange, (e.g.: "Case.Responses")
    :param binding_key: The binding key to associate the exchange and queue (e.g.: "Case.Responses.binding")
    :param queue_args: Arguments passed to the rabbitmq queue declaration
    """
    rabbitmq_connection = _create_connection()
    channel = rabbitmq_connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
    channel.queue_declare(queue=queue_name, durable=True, arguments=queue_args)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)
    logger.info('Successfully initialised rabbitmq', exchange=exchange_name, binding=binding_key)


def send_message_to_rabbitmq(message,
                             exchange_name=RABBIT_EXCHANGE,
                             routing_key=RABBIT_ROUTE):
    """
    Send message to rabbitmq

    :param message: The message to send to the queue in JSON format
    :param exchange_name: The rabbitmq exchange to publish to, (e.g.: "case-outbound-exchange")
    :param routing_key: The direct route to a queue the message should be sent to (e.g.: "Case.Responses.binding")
    :return: boolean
    :raises: PublishMessageError
    """
    rabbitmq_connection = _create_connection()
    rabbitmq_channel = rabbitmq_connection.channel()
    rabbitmq_channel.basic_publish(exchange=exchange_name,
                                   routing_key=routing_key,
                                   body=str(message),
                                   properties=pika.BasicProperties(content_type='text/xml'))
    logger.info('Message successfully sent to rabbitmq', exchange=exchange_name, route=routing_key)

    rabbitmq_connection.close()


def _create_connection(username=RABBIT_USERNAME,
                       password=RABBIT_PASSWORD,
                       host=RABBIT_HOST,
                       port=RABBIT_PORT,
                       virtualhost=RABBIT_VIRTUALHOST):
    credentials = pika.PlainCredentials(RABBIT_USERNAME, RABBIT_PASSWORD)
    parameters = pika.ConnectionParameters(RABBIT_HOST, RABBIT_PORT, RABBIT_VIRTUALHOST, credentials)

    logger.debug('Connecting to rabbitmq', url=parameters.host)
    return pika.BlockingConnection(parameters)
