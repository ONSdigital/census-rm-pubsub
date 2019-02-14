from unittest.mock import patch, MagicMock

from pika.exceptions import AMQPConnectionError
from pytest import raises

from app.rabbit_helper import init_rabbitmq, send_message_to_rabbitmq
from test.helpers.when_then_return import create_when_then_return

CHANNEL = 'test channel'

GOOD_URL = 'good_url'

RM_RABBIT_AMQP = "amqp://user:password@host:0000"
RM_RABBIT_QUEUE = "Test.Queue"
RM_RABBIT_CONNECTION = 'rabbit connection'


def test_rabbit_init():
    with patch('app.rabbit_helper.pika') as mock_pika:
        connection_mock = MagicMock()
        mock_pika.URLParameters = create_when_then_return(RM_RABBIT_AMQP, return_value=GOOD_URL)
        mock_pika.BlockingConnection = create_when_then_return(GOOD_URL, return_value=connection_mock)
        init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE)
        connection_mock.channel.assert_called_once()


def test_initialise_messaging_rabbit_fails():
    with raises(AMQPConnectionError):
        with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
            init_rabbitmq()