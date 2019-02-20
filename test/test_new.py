import asyncio
import time
import uuid
from unittest import TestCase

import pika
from google.api_core.exceptions import AlreadyExists, PermissionDenied
from google.cloud import pubsub_v1

from test.scripts import create_topic
from coverage.python import os

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

# TODO, Not really sure why this is important, but blows up without it. Try removing 1 at a time to figure out which and why?

os.environ["RABBIT_AMQP"] = "amqp://guest:guest@localhost:35672"
os.environ["SUBSCRIPTION_PROJECT_ID"] = "project"
os.environ["RECEIPT_TOPIC_PROJECT_ID"] = "project"
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8538"
os.environ["RABBIT_QUEUE"] = "Case.Responses"
os.environ["RABBIT_EXCHANGE"] = "case-outbound-exchange"
os.environ["RECEIPT_TOPIC_NAME"] = "eq-submission-topic"
os.environ["SUBSCRIPTION_NAME"] = "rm-receipt-subscription"


class CensusRMPubSubComponentTest(TestCase):
    # callback_called = False

    msg_body = ""

    def test_e2e_good(self):
        channel, queue_declare_result = init_rabbitmq()
        channel.queue_purge(queue=RABBIT_QUEUE)
        create_topic(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)
        create_subscription(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME, SUBSCRIPTION_NAME)

        published_message = publish_to_pubsub(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)

        channel, queue_declare_result = init_rabbitmq()

        self.msg_body = ""

        def callback(ch, method, properties, body):
            print(" [x] %r" % body)
            self.msg_body = body
            channel.stop_consuming()

        assert queue_declare_result.method.message_count == 1
        res = channel.basic_get(queue=RABBIT_QUEUE)

        #
        # channel.basic_consume(callback,
        #                       queue=RABBIT_QUEUE,
        #                       no_ack=True)
        # channel.start_consuming()
        #

        # while True:
        #     if self.msg_body == "":
        #         asyncio.sleep(1)



        expected_msg = b'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><ns2:caseReceipt xmlns:ns2="http://ons.gov.uk/ctp/response/casesvc/message/feedback"><caseId>c559e2ad-4be2-4af6-91f9-6f51158ee555</caseId><inboundChannel>OFFLINE</inboundChannel><responseDateTime>2008-08-24T00:00:00+00:00</responseDateTime></ns2:caseReceipt>'

        # TODO Not matching? don't why
        assert expected_msg == self.msg_body


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
