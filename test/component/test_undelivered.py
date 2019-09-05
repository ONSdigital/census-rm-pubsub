import json
import time
import uuid
from unittest import TestCase

import pika
from coverage.python import os
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

RABBIT_AMQP = "amqp://guest:guest@localhost:35672"
PPO_UNDELIVERED_TOPIC_PROJECT_ID = "ppo-undelivered-project"
QM_UNDELIVERED_TOPIC_PROJECT_ID = "qm-undelivered-project"
RABBIT_EXCHANGE = "events"
PPO_UNDELIVERED_TOPIC_NAME = "ppo-undelivered-mail-topic"
QM_UNDELIVERED_TOPIC_NAME = "qm-undelivered-mail-topic"
UNDELIVERED_RABBIT_TEST_QUEUE = "test.undelivered"
UNDELIVERED_RABBIT_ROUTE = "event.fulfilment.undelivered"


class CensusRMPubSubComponentTest(TestCase):

    def setUp(self):
        os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8539"
        self.purge_rabbit_queues()

    def test_ppo_undelivered_e2e_with_successful_msg(self):
        self.purge_rabbit_queues()
        expected_case_ref = 1234
        expected_product_code = 'P_OR_H1'
        self.publish_ppo_undelivered_to_pubsub(expected_case_ref, expected_product_code)

        self.init_rabbitmq(binding_key=UNDELIVERED_RABBIT_ROUTE, queue_name=UNDELIVERED_RABBIT_TEST_QUEUE)
        assert self.queue_declare_result.method.message_count == 1, "Expected 1 message to be on rabbitmq queue"

        undelivered_msg = self.get_msg_body_from_rabbit(UNDELIVERED_RABBIT_TEST_QUEUE)
        actual_result = json.loads(undelivered_msg)
        assert actual_result['event']['type'] == 'UNDELIVERED_MAIL_REPORTED'
        assert actual_result['event']['source'] == 'RECEIPT_SERVICE'
        assert actual_result['event']['channel'] == 'PPO'
        assert actual_result['event']['dateTime'] == '2019-08-03T14:30:01Z'
        assert actual_result['event']['transactionId'] == '1'
        assert actual_result['payload']['fulfilmentInformation']['caseRef'] == expected_case_ref
        assert actual_result['payload']['fulfilmentInformation']['productCode'] == expected_product_code

    def test_qm_undelivered_e2e_with_successful_msg(self):
        self.purge_rabbit_queues()
        expected_q_id = str(uuid.uuid4())
        self.publish_qm_undelivered_to_pubsub(expected_q_id)

        self.init_rabbitmq(binding_key=UNDELIVERED_RABBIT_ROUTE, queue_name=UNDELIVERED_RABBIT_TEST_QUEUE)
        assert self.queue_declare_result.method.message_count == 1, "Expected 1 message to be on rabbitmq queue"

        undelivered_msg = self.get_msg_body_from_rabbit(UNDELIVERED_RABBIT_TEST_QUEUE)
        actual_result = json.loads(undelivered_msg)
        assert actual_result['event']['type'] == 'UNDELIVERED_MAIL_REPORTED'
        assert actual_result['event']['source'] == 'RECEIPT_SERVICE'
        assert actual_result['event']['channel'] == 'QM'
        assert actual_result['event']['dateTime'] == '2019-08-03T14:30:01Z'
        assert actual_result['event']['transactionId'] == '1'
        assert actual_result['payload']['fulfilmentInformation']['questionnaireId'] == expected_q_id

    def purge_rabbit_queues(self):
        self.init_rabbitmq()
        self.channel.queue_purge(UNDELIVERED_RABBIT_TEST_QUEUE)

    def get_msg_body_from_rabbit(self, rabbit_queue):
        actual_msg = self.channel.basic_get(rabbit_queue)
        return actual_msg[2].decode('utf-8')

    def publish_ppo_undelivered_to_pubsub(self, case_ref, product_code):
        publisher = pubsub_v1.PublisherClient()

        topic_path = publisher.topic_path(PPO_UNDELIVERED_TOPIC_PROJECT_ID, PPO_UNDELIVERED_TOPIC_NAME)

        datadict = {"transactionId": "1",
                    "dateTime": "2019-08-03T14:30:01Z",
                    "caseRef": case_ref,
                    "productCode": product_code,
                    "channel": "PPO",
                    "type": "UNDELIVERED_MAIL_REPORTED"}

        data = json.dumps(datadict)

        future = publisher.publish(topic_path,
                                   data=data.encode('utf-8'))
        if not future.done():
            time.sleep(1)
        try:
            future.result(timeout=30)
        except GoogleAPIError:
            assert False, "Failed to publish message to pubsub"

        print(f'Message published to {topic_path}')

    def publish_qm_undelivered_to_pubsub(self, q_id):
        publisher = pubsub_v1.PublisherClient()

        topic_path = publisher.topic_path(QM_UNDELIVERED_TOPIC_PROJECT_ID, QM_UNDELIVERED_TOPIC_NAME)

        datadict = {"transactionId": "1",
                    "dateTime": "2019-08-03T14:30:01Z",
                    "questionnaireId": q_id}

        data = json.dumps(datadict)

        future = publisher.publish(topic_path,
                                   data=data.encode('utf-8'))
        if not future.done():
            time.sleep(1)
        try:
            future.result(timeout=30)
        except GoogleAPIError:
            assert False, "Failed to publish message to pubsub"

        print(f'Message published to {topic_path}')

    def init_rabbitmq(self, rabbitmq_amqp=RABBIT_AMQP,
                      binding_key=UNDELIVERED_RABBIT_ROUTE,
                      exchange_name=RABBIT_EXCHANGE,
                      queue_name=UNDELIVERED_RABBIT_TEST_QUEUE):
        # NB: instead of pre-loading a definitions.json file we have programmatically declared what we need for the test
        rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_amqp))
        channel = rabbitmq_connection.channel()
        channel.exchange_declare(exchange=exchange_name, exchange_type='topic', durable=True)
        queue_declare_result = channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)
        self.channel = channel
        self.queue_declare_result = queue_declare_result
