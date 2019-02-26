import os
from unittest.mock import patch, MagicMock
from unittest import TestCase

from pika.exceptions import AMQPConnectionError
from pytest import raises

from test import create_stub_function


RABBIT_AMQP = "amqp://user:pa55word@host:0001"
BINDING_KEY = "Test.binding"
RABBIT_QUEUE = "Test.Queue"
RABBIT_EXCHANGE = "test-exchange"


class RabbitHelperTestCase(TestCase):
    property_class = 'property_class'
    rabbit_url = 'rabbit_url'
    rabbit_connection = 'rabbit connection'
    message = "xml message<blah>"
    queue_args = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                  'x-dead-letter-routing-key': BINDING_KEY}

    def setUp(self):
        test_environment_variables = {
            'RABBIT_AMQP': RABBIT_AMQP,
            'RABBIT_ROUTING_KEY': BINDING_KEY,
            'RABBIT_QUEUE': RABBIT_QUEUE,
            'RABBIT_EXCHANGE': RABBIT_EXCHANGE,
        }
        os.environ.update(test_environment_variables)

    def test_rabbit_init(self):
        from app.rabbit_helper import init_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_stub_function(RABBIT_AMQP, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_stub_function(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)

            init_rabbitmq(rabbitmq_amqp=RABBIT_AMQP,
                          binding_key=BINDING_KEY,
                          exchange_name=RABBIT_EXCHANGE,
                          queue_name=RABBIT_QUEUE,
                          queue_args=self.queue_args)

            channel_mock.exchange_declare.assert_called_once_with(exchange=RABBIT_EXCHANGE, exchange_type='direct',
                                                                  durable=True)
            channel_mock.queue_declare.assert_called_once_with(arguments=self.queue_args, durable=True,
                                                               queue=RABBIT_QUEUE)
            channel_mock.queue_bind.assert_called_once_with(exchange=RABBIT_EXCHANGE,
                                                            queue=RABBIT_QUEUE,
                                                            routing_key=BINDING_KEY)

    def test_initialise_messaging_rabbit_fails(self):
        from app.rabbit_helper import init_rabbitmq

        with raises(AMQPConnectionError):
            with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
                init_rabbitmq()

    def test_send_to_rabbitmq_queue(self):
        from app.rabbit_helper import send_message_to_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_stub_function(RABBIT_AMQP, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_stub_function(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)
            mock_pika.BasicProperties = create_stub_function(expected_kwargs={'content_type': 'text/xml'},
                                                             return_value=self.property_class)

            send_message_to_rabbitmq(self.message,
                                     rabbitmq_amqp=RABBIT_AMQP,
                                     exchange_name=RABBIT_EXCHANGE,
                                     routing_key=BINDING_KEY)

            channel_mock.basic_publish.assert_called_once_with(exchange=RABBIT_EXCHANGE,
                                                               routing_key=BINDING_KEY,
                                                               body=str(self.message),
                                                               properties=self.property_class)
            connection_mock.close.assert_called_once()
