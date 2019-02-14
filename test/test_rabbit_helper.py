from unittest.mock import patch

from pika.exceptions import AMQPConnectionError
from pytest import raises

from app.rabbit_helper import init_rabbitmq, send_message_to_rabbitmq

RM_RABBIT_AMQP = "amqp://user:password@host:0000"
RM_RABBIT_QUEUE = "Test.Queue"


def test_rabbit_init():
    with patch('pika.BlockingConnection'):
        init_rabbitmq(rabbitmq_amqp=RM_RABBIT_AMQP, queue_name=RM_RABBIT_QUEUE)
        # Now test that the queue is declared??


def test_initialise_messaging_rabbit_fails():
    with raises(AMQPConnectionError):
        with patch('pika.BlockingConnection', side_effect=AMQPConnectionError):
            init_rabbitmq()


def test_send_message_to_queue():
    with patch('pika.BlockingConnection'):
        send_message_to_rabbitmq('TestMsg')


# def test_send_message_fails():
#     TODO
