import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from pika.exceptions import AMQPConnectionError
from pytest import raises

from test import create_stub_function


class RabbitHelperTestCase(TestCase):
    rabbit_username = 'user'
    rabbit_password = 'pa55word'
    rabbit_host = 'host'
    rabbit_port = '0001'
    rabbit_virtualhost = '/'

    binding_key = "test.binding"
    rabbit_exchange = "test-exchange"
    property_class = 'property_class'
    rabbit_url = 'rabbit_url'
    rabbit_connection = 'rabbit connection'
    message = "message test"

    def setUp(self):
        test_environment_variables = {
            'RABBIT_USERNAME': self.rabbit_username,
            'RABBIT_PASSWORD': self.rabbit_password,
            'RABBIT_HOST': self.rabbit_host,
            'RABBIT_PORT': self.rabbit_port,
            'RABBIT_VIRTUALHOST': self.rabbit_virtualhost,
            'RABBIT_ROUTING_KEY': self.binding_key,
            'RABBIT_EXCHANGE': self.rabbit_exchange,
        }
        os.environ.update(test_environment_variables)

    def test_rabbit_init(self):
        from app.rabbit_helper import init_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.BlockingConnection = create_stub_function(mock_pika.ConnectionParameters.return_value,
                                                                return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)

            init_rabbitmq(exchange_name=self.rabbit_exchange)

            mock_pika.PlainCredentials.assert_called_once_with(self.rabbit_username, self.rabbit_password)
            mock_pika.ConnectionParameters.assert_called_once_with(
                self.rabbit_host, self.rabbit_port, self.rabbit_virtualhost, mock_pika.PlainCredentials.return_value)

    def test_initialise_messaging_rabbit_fails(self):
        from app.rabbit_helper import init_rabbitmq

        with raises(AMQPConnectionError):
            with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
                init_rabbitmq()

    def test_send_to_rabbitmq_exchange(self):
        from app.rabbit_helper import send_message_to_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.BlockingConnection = create_stub_function(mock_pika.ConnectionParameters.return_value,
                                                                return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_stub_function(return_value=channel_mock)
            mock_pika.BasicProperties = \
                create_stub_function(expected_kwargs={'content_type': 'application/json', 'delivery_mode': 2},
                                     return_value=self.property_class)

            send_message_to_rabbitmq(self.message,
                                     exchange_name=self.rabbit_exchange,
                                     routing_key=self.binding_key)

            mock_pika.PlainCredentials.assert_called_once_with(self.rabbit_username, self.rabbit_password)
            mock_pika.ConnectionParameters.assert_called_once_with(
                self.rabbit_host, self.rabbit_port, self.rabbit_virtualhost, mock_pika.PlainCredentials.return_value)

            channel_mock.basic_publish.assert_called_once_with(exchange=self.rabbit_exchange,
                                                               routing_key=self.binding_key,
                                                               body=str(self.message),
                                                               properties=self.property_class)
            connection_mock.close.assert_called_once()
