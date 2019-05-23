import os
from unittest.mock import patch, MagicMock
from unittest import TestCase

from pika.exceptions import AMQPConnectionError
from pytest import raises

from test import create_stub_function


class RabbitHelperTestCase(TestCase):

    rabbit_amqp = "amqp://user:pa55word@host:0001"
    binding_key = "test.binding"
    rabbit_queue = "test.queue"
    rabbit_exchange = "test-exchange"
    property_class = 'property_class'
    rabbit_url = 'rabbit_url'
    rabbit_connection = 'rabbit connection'
    message = "message test"

    def setUp(self):
        test_environment_variables = {
            'RABBIT_AMQP': self.rabbit_amqp,
            'RABBIT_ROUTING_KEY': self.binding_key,
            'RABBIT_QUEUE': self.rabbit_queue,
            'RABBIT_EXCHANGE': self.rabbit_exchange,
        }
        os.environ.update(test_environment_variables)

    def test_rabbit_init(self):
        from app.rabbit_helper import init_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_stub_function(self.rabbit_amqp, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_stub_function(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)

            init_rabbitmq(rabbitmq_amqp=self.rabbit_amqp,
                          binding_key=self.binding_key,
                          exchange_name=self.rabbit_exchange,
                          queue_name=self.rabbit_queue)

            channel_mock.exchange_declare.assert_called_once_with(exchange=self.rabbit_exchange, exchange_type='direct',
                                                                  durable=True)
            channel_mock.queue_declare.assert_called_once_with(durable=True,
                                                               queue=self.rabbit_queue)
            channel_mock.queue_bind.assert_called_once_with(exchange=self.rabbit_exchange,
                                                            queue=self.rabbit_queue,
                                                            routing_key=self.binding_key)

    def test_initialise_messaging_rabbit_fails(self):
        from app.rabbit_helper import init_rabbitmq

        with raises(AMQPConnectionError):
            with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
                init_rabbitmq()

    def test_send_to_rabbitmq_queue(self):
        from app.rabbit_helper import send_message_to_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_stub_function(self.rabbit_amqp, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_stub_function(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)
            mock_pika.BasicProperties = create_stub_function(expected_kwargs={'content_type': 'application/json'},
                                                             return_value=self.property_class)

            send_message_to_rabbitmq(self.message,
                                     rabbitmq_amqp=self.rabbit_amqp,
                                     exchange_name=self.rabbit_exchange,
                                     routing_key=self.binding_key)

            channel_mock.basic_publish.assert_called_once_with(exchange=self.rabbit_exchange,
                                                               routing_key=self.binding_key,
                                                               body=str(self.message),
                                                               properties=self.property_class)
            connection_mock.close.assert_called_once()
