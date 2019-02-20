import time
import uuid
from unittest import TestCase

import pika
from coverage.python import os
from google.cloud import pubsub_v1
from psutil.tests import retry

from test.DataNotThereYet import DataNotYetThereError

os.environ["RABBIT_AMQP"] = "amqp://guest:guest@localhost:35672"
os.environ["SUBSCRIPTION_PROJECT_ID"] = "project"
os.environ["RECEIPT_TOPIC_PROJECT_ID"] = "project"
os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8538"
os.environ["RABBIT_QUEUE"] = "Case.Responses.binding"
os.environ["RABBIT_EXCHANGE"] = "case-outbound-exchange"
os.environ["RECEIPT_TOPIC_NAME"] = "eq-submission-topic"
os.environ["SUBSCRIPTION_NAME"] = "rm-receipt-subscription"

RM_RABBIT_AMQP = os.getenv("RABBIT_AMQP", "amqp://guest:guest@localhost:6672")
RM_RABBIT_EXCHANGE = os.getenv("RABBIT_EXCHANGE", "case-outbound-exchange")
RM_RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "Case.Responses")
RM_RABBIT_ROUTE = os.getenv("RABBIT_ROUTING_KEY", "Case.Responses.binding")
RM_RABBIT_QUEUE_ARGS = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                        'x-dead-letter-routing-key': RM_RABBIT_ROUTE}


# def publish_to_pubsub(receipt_topic_project_id, receipt_topic_name):
#     publisher = pubsub_v1.PublisherClient()
#     try:
#         topic_path = publisher.topic_path(receipt_topic_project_id, receipt_topic_name)
#     except IndexError:
#         print('Usage: python publish_message.py PROJECT_ID TOPIC_ID')
#
#     data = '{"timeCreated":"2008-08-24T00:00:00Z"}'.encode('utf-8')
#
#     future = publisher.publish(topic_path,
#                                data=data,
#                                eventType='OBJECT_FINALIZE',
#                                bucketId='123',
#                                objectId=str(uuid.uuid4()))
#
#     while not future.done():
#         time.sleep(1)
#
#     print(f'Message published to {topic_path}')
#
#
# callBackCalled = False
#
#
# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body)


# @retry
# def wait_for_callback():
#     if callBackCalled:
#         raise DataNotYetThereError

class XRabbitHelperTestCase(TestCase):

    def test_rabbit_init(self):
        assert True

#
# class CensusRMPubSubComponentTest(TestCase):
#
#     def test_e2e_good(self):
#         # This test presumes that the rabbit and pubsub docker images are running
#         # and that the queues are set up
#
#         rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(RM_RABBIT_AMQP))
#         channel = rabbitmq_connection.channel()
#         # channel.basic_consume(callback,
#         #                       queue=RM_RABBIT_QUEUE,
#         #                       no_ack=True)
#
#         # publish_to_pubsub("project", "eq-submission-topic")
#
#         # wait_for_callback()
#
#         # will then publish a msg onto the pubsub topic
#         # will then check that the msg is on the rabbit queue
#         # Might have to do some stuff in a different thread, because setting up a subscription may not work in time?
#         # Or we just have a retry thing, so 'getMessageCount()' has the retry thing same as acceptance tests? Don't return to assert until
#         # the message has arrived or 30 secs has passed
#         assert 1 == 1
