from unittest.mock import patch, MagicMock

from pika.exceptions import AMQPConnectionError
from pytest import raises

from app.rabbit_helper import init_rabbitmq
from test.helpers.when_then_return import create_when_then_return

RM_RABBIT_URL = 'rabbit_url'
RM_RABBIT_AMQP = "amqp://user:pa55word@host:0001"
RM_BINDING_KEY = "test-binding-key"
RM_RABBIT_QUEUE = "Test.Queue"
RM_RABBIT_CONNECTION = 'rabbit connection'
RM_RABBIT_EXCHANGE = "test-exchange"
RM_RABBIT_QUEUE_ARGS = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                        'x-dead-letter-routing-key': RM_BINDING_KEY}


def test_rabbit_init():
    with patch('app.rabbit_helper.pika') as mock_pika:
        connection_mock = MagicMock()
        mock_pika.URLParameters = create_when_then_return(RM_RABBIT_AMQP, return_value=RM_RABBIT_URL)
        mock_pika.BlockingConnection = create_when_then_return(RM_RABBIT_URL, return_value=connection_mock)

        channel_mock = MagicMock()
        connection_mock.channel = create_when_then_return(return_value=channel_mock)

        init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP,
                      binding_key=RM_BINDING_KEY,
                      exchange_name=RM_RABBIT_EXCHANGE,
                      queue_name=RM_RABBIT_QUEUE)

        channel_mock.exchange_declare.assert_called_once_with(exchange=RM_RABBIT_EXCHANGE, exchange_type='direct',
                                                              durable=True)
        channel_mock.queue_declare.assert_called_once_with(queue=RM_RABBIT_QUEUE, durable=True,
                                                          arguments=RM_RABBIT_QUEUE_ARGS)
        channel_mock.queue_bind(exchange=RM_RABBIT_EXCHANGE, queue=RM_RABBIT_QUEUE, routing_key=RM_BINDING_KEY)


def test_initialise_messaging_rabbit_fails():
    with raises(AMQPConnectionError):
        with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
            init_rabbitmq()
