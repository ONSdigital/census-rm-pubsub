import os
from unittest.mock import patch, MagicMock
from unittest import TestCase

from pika.exceptions import AMQPConnectionError
from pytest import raises

from test.helpers.when_then_return import create_when_then_return, create_when_then_return_kwargs


RM_RABBIT_AMQP = "amqp://user:pa55word@host:0001"
RM_BINDING_KEY = "Test.binding"
RM_RABBIT_QUEUE = "Test.Queue"
RM_RABBIT_EXCHANGE = "test-exchange"


class RabbitHelperTestCase(TestCase):

    property_class = 'property_class'
    rabbit_url = 'rabbit_url'
    rabbit_connection = 'rabbit connection'
    message = "xml message<blah>"
    queue_args = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                  'x-dead-letter-routing-key': RM_BINDING_KEY}

    def setUp(self):
        test_environment_variables = {
            'RABBIT_AMQP': RM_RABBIT_AMQP,
            'RABBIT_ROUTING_KEY': RM_BINDING_KEY,
            'RABBIT_QUEUE': RM_RABBIT_QUEUE,
            'RABBIT_EXCHANGE': RM_RABBIT_EXCHANGE,
        }
        os.environ.update(test_environment_variables)

    def test_rabbit_init(self):
        from app.rabbit_helper import init_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_when_then_return(RM_RABBIT_AMQP, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_when_then_return(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_when_then_return(return_value=channel_mock)

            init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP,
                          binding_key=RM_BINDING_KEY,
                          exchange_name=RM_RABBIT_EXCHANGE,
                          queue_name=RM_RABBIT_QUEUE,
                          queue_args=self.queue_args)

            channel_mock.exchange_declare.assert_called_once_with(exchange=RM_RABBIT_EXCHANGE, exchange_type='direct',
                                                                  durable=True)
            channel_mock.queue_declare.assert_called_once_with(arguments=self.queue_args, durable=True,
                                                               queue=RM_RABBIT_QUEUE)
            channel_mock.queue_bind(exchange=RM_RABBIT_EXCHANGE, queue=RM_RABBIT_QUEUE, routing_key=RM_BINDING_KEY)

    def test_initialise_messaging_rabbit_fails(self):
        from app.rabbit_helper import init_rabbitmq

        with raises(AMQPConnectionError):
            with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
                init_rabbitmq()

    def test_send_to_rabbitmq_queue(self):
        from app.rabbit_helper import send_message_to_rabbitmq

        with patch('app.rabbit_helper.pika') as mock_pika:
            connection_mock = MagicMock()
            mock_pika.URLParameters = create_when_then_return(RM_RABBIT_AMQP, return_value=self.rabbit_url)
            mock_pika.BlockingConnection = create_when_then_return(self.rabbit_url, return_value=connection_mock)

            channel_mock = MagicMock()
            connection_mock.channel = create_when_then_return(return_value=channel_mock)
            mock_pika.BasicProperties = create_when_then_return_kwargs({'content_type': 'text/xml'},
                                                                       return_value=self.property_class)

            send_message_to_rabbitmq(self.message,
                                     rabbitmq_amqp=RM_RABBIT_AMQP,
                                     exchange_name=RM_RABBIT_EXCHANGE,
                                     routing_key=RM_BINDING_KEY)

            channel_mock.basic_publish.assert_called_once_with(exchange=RM_RABBIT_EXCHANGE,
                                                               routing_key=RM_BINDING_KEY,
                                                               body=str(self.message),
                                                               properties=self.property_class)
            connection_mock.close.assert_called_once()


# TODO
# do we need to test any errors here really? presently the App does not deal with them.  Presume it would crash and attempt restart?
# Can't really do a lot if rabbit or pubsub not there
