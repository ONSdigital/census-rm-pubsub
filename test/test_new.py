import time
import uuid
from unittest import TestCase

import pika
from google.api_core.exceptions import AlreadyExists, PermissionDenied
from google.cloud import pubsub_v1

from app.rabbit_helper import send_message_to_rabbitmq, init_rabbitmq
from test.scripts import create_topic

RABBIT_AMQP = "amqp://guest:guest@localhost:35672"
SUBSCRIPTION_PROJECT_ID = "project"
RECEIPT_TOPIC_PROJECT_ID = "project"
PUBSUB_EMULATOR_HOST = "localhost:8538"
RABBIT_QUEUE = "Case.Responses"
RABBIT_EXCHANGE = "case-outbound-exchange"
RABBIT_ROUTE = "Case.Responses.binding"
RECEIPT_TOPIC_NAME = "eq-submission-topic"
SUBSCRIPTION_NAME = "rm-receipt-subscription"
RABBIT_QUEUE_ARGS = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                     'x-dead-letter-routing-key': RABBIT_ROUTE}


def publish_to_pubsub(receipt_topic_project_id, receipt_topic_name):
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(receipt_topic_project_id, receipt_topic_name)
    except IndexError:
        print('Usage: python publish_message.py PROJECT_ID TOPIC_ID')
        assert False

    data = '{"timeCreated":"2008-08-24T00:00:00Z"}'.encode('utf-8')

    future = publisher.publish(topic_path,
                               data=data,
                               eventType='OBJECT_FINALIZE',
                               bucketId='123',
                               objectId=str(uuid.uuid4()))

    while not future.done():
        time.sleep(1)

    print(f'Message published to {topic_path}')

    return data


def init_rabbitmq(rabbitmq_amqp=RABBIT_AMQP,
                  binding_key=RABBIT_ROUTE,
                  exchange_name=RABBIT_EXCHANGE,
                  queue_name=RABBIT_QUEUE,
                  queue_args=RABBIT_QUEUE_ARGS):
    rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_amqp))
    channel = rabbitmq_connection.channel()
    channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
    queue_declare_result = channel.queue_declare(queue=queue_name, durable=True, arguments=queue_args)
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)
    return channel, queue_declare_result


class CensusRMPubSubComponentTest(TestCase):
    # callback_called = False

    def test_e2e_good(self):
        create_topic(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)
        create_subscription(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME, SUBSCRIPTION_NAME)

        published_message = publish_to_pubsub(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)

        channel, queue_declare_result = init_rabbitmq()
        channel.queue_purge(queue=RABBIT_QUEUE)

        assert queue_declare_result.method.message_count == 1


def create_topic(a, b):
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(a, b)
    except IndexError:
        print('Usage: python create_topic.py PROJECT_ID TOPIC_ID')

    try:
        topic = publisher.create_topic(topic_path)
    except AlreadyExists:
        print('topic already exists')
    else:
        print(f'Topic created: {topic}')


def create_subscription(a, b, c):
    subscriber = pubsub_v1.SubscriberClient()
    try:
        topic_path = subscriber.topic_path(a, b)
        subscription_path = subscriber.subscription_path(a, c)
    except IndexError:
        print('Usage: python create_subscription.py PROJECT_ID TOPIC_ID SUBSCRIPTION_ID')
    except AlreadyExists:
        print('Subscription already exists')

    try:
        sub = subscriber.create_subscription(subscription_path, topic_path)
    except AlreadyExists:
        print('Subscription already exists')
    except PermissionDenied:
        print('Subscription can not be created')
    else:
        print(f'Subscription created: {sub}')
