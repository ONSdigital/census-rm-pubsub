import json
import time
import uuid
from unittest import TestCase

import pika
from coverage.python import os
from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

RABBIT_AMQP = "amqp://guest:guest@localhost:35672"
RECEIPT_TOPIC_PROJECT_ID = "project"
RABBIT_QUEUE = "Case.Responses"
RABBIT_EXCHANGE = "case-outbound-exchange"
RABBIT_ROUTE = "Case.Responses.binding"
RECEIPT_TOPIC_NAME = "eq-submission-topic"


class CensusRMPubSubComponentTest(TestCase):

    def setUp(self):
        os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8539"
        self.purge_rabbit_queue()

    def test_e2e_with_sucessful_msg(self):
        expected_case_id = str(uuid.uuid4())
        expected_tx_id = str(uuid.uuid4())
        expected_q_id = str(uuid.uuid4())
        self.publish_to_pubsub(expected_tx_id, expected_q_id, expected_case_id)

        expected_msg = json.dumps({
            "event": {
                "type": "RESPONSE_RECEIVED",
                "source": "RECEIPT_SERVICE",
                "channel": "EQ",
                "dateTime": "2008-08-24T00:00:00+00:00",
                "transactionId": expected_tx_id
            },
            "payload": {
                "response": {
                    "caseId": expected_case_id,
                    "questionnaireId": expected_q_id,
                    "unreceipt": False
                }
            }
        })

        self.init_rabbitmq()
        assert self.queue_declare_result.method.message_count == 1, "Expected 1 message to be on rabbitmq queue"

        actual_msg_body_str = self.get_msg_body_from_rabbit(self.channel)
        assert expected_msg == actual_msg_body_str, "RabbitMQ message text incorrect"

    def test_e2e_with_no_case_id(self):
        expected_tx_id = str(uuid.uuid4())
        expected_q_id = str(uuid.uuid4())
        self.publish_to_pubsub(expected_tx_id, expected_q_id)

        expected_msg = json.dumps({
            "event": {
                "type": "RESPONSE_RECEIVED",
                "source": "RECEIPT_SERVICE",
                "channel": "EQ",
                "dateTime": "2008-08-24T00:00:00+00:00",
                "transactionId": expected_tx_id
            },
            "payload": {
                "response": {
                    "caseId": None,
                    "questionnaireId": expected_q_id,
                    "unreceipt": False
                }
            }
        })

        self.init_rabbitmq()
        time.sleep(1)
        assert self.queue_declare_result.method.message_count == 1, "Expected 1 message to be on rabbitmq queue"

        actual_msg_body_str = self.get_msg_body_from_rabbit(self.channel)
        assert expected_msg == actual_msg_body_str, "RabbitMQ message text incorrect"

    def purge_rabbit_queue(self):
        self.init_rabbitmq()
        self.channel.queue_purge(queue=RABBIT_QUEUE)

    def get_msg_body_from_rabbit(self, channel):
        actual_msg = channel.basic_get(queue=RABBIT_QUEUE)
        return actual_msg[2].decode('utf-8')

    def publish_to_pubsub(self, tx_id, questionnaire_id, case_id=None):
        publisher = pubsub_v1.PublisherClient()

        topic_path = publisher.topic_path(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)

        datadict = {
            "timeCreated": "2008-08-24T00:00:00Z",
            "metadata": {
                "tx_id": tx_id,
                "questionnaire_id": questionnaire_id,
            }
        }

        if case_id is not None:
            datadict["metadata"]["case_id"] = case_id

        data = json.dumps(datadict)

        future = publisher.publish(topic_path,
                                   data=data.encode('utf-8'),
                                   eventType='OBJECT_FINALIZE',
                                   bucketId='123',
                                   objectId=tx_id)
        if not future.done():
            time.sleep(1)
        try:
            future.result(timeout=30)
        except GoogleAPIError:
            assert False, "Failed to publish message to pubsub"

        print(f'Message published to {topic_path}')

    def init_rabbitmq(self, rabbitmq_amqp=RABBIT_AMQP,
                      binding_key=RABBIT_ROUTE,
                      exchange_name=RABBIT_EXCHANGE,
                      queue_name=RABBIT_QUEUE):
        rabbitmq_connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_amqp))
        channel = rabbitmq_connection.channel()
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
        queue_declare_result = channel.queue_declare(queue=queue_name, durable=True)
        channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)
        self.channel = channel
        self.queue_declare_result = queue_declare_result
        return
