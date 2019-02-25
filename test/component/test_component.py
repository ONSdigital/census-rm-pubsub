import json
import time
import uuid
from unittest import TestCase

import pika
from coverage.python import os
from google.cloud import pubsub_v1

RABBIT_AMQP = "amqp://guest:guest@localhost:35672"
RECEIPT_TOPIC_PROJECT_ID = "project"
RABBIT_QUEUE = "Case.Responses"
RABBIT_EXCHANGE = "case-outbound-exchange"
RABBIT_ROUTE = "Case.Responses.binding"
RECEIPT_TOPIC_NAME = "eq-submission-topic"
RABBIT_QUEUE_ARGS = {'x-dead-letter-exchange': 'case-deadletter-exchange',
                     'x-dead-letter-routing-key': RABBIT_ROUTE}


class CensusRMPubSubComponentTest(TestCase):

    def setUp(self):
        os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8538"
        self.purge_rabbit_queue()

    def test_e2e_with_sucessful_msg(self):
        expected_case_id = str(uuid.uuid4())
        expected_tx_id = str(uuid.uuid4())
        self.publish_to_pubsub(expected_tx_id, expected_case_id)

        expected_msg = (
            f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            f'<ns2:caseReceipt xmlns:ns2="http://ons.gov.uk/ctp/response/casesvc/message/feedback">'
            f'<caseId>{expected_case_id}</caseId><inboundChannel>OFFLINE</inboundChannel>'
            f'<responseDateTime>2008-08-24T00:00:00+00:00</responseDateTime></ns2:caseReceipt>'
        )

        channel, queue_declare_result = self.init_rabbitmq()
        assert queue_declare_result.method.message_count == 1, "Expected 1 msg to be on rabbitmq queue"

        actual_msg_body_str = self.get_msg_body_from_rabbit(channel)
        assert expected_msg == actual_msg_body_str, "Msg text incorrect"

    def purge_rabbit_queue(self):
        channel, queue_declare_result = self.init_rabbitmq()
        channel.queue_purge(queue=RABBIT_QUEUE)

    def get_msg_body_from_rabbit(self, channel):
        actual_msg = channel.basic_get(queue=RABBIT_QUEUE)
        return actual_msg[2].decode('utf-8')

    def publish_to_pubsub(self, tx_id, case_id):
        publisher = pubsub_v1.PublisherClient()

        try:
            topic_path = publisher.topic_path(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME)
        except IndexError:
            print('Usage: python publish_message.py PROJECT_ID TOPIC_ID')
            return

        data = json.dumps({
            "timeCreated": "2008-08-24T00:00:00Z",
            "metadata": {
                "case_id": case_id,
                "tx_id": tx_id,
            }
        })

        future = publisher.publish(topic_path,
                                   data=data.encode('utf-8'),
                                   eventType='OBJECT_FINALIZE',
                                   bucketId='123',
                                   objectId=tx_id)
        while not future.done():
            time.sleep(1)

        print(f'Message published to {topic_path}')

    def init_rabbitmq(self, rabbitmq_amqp=RABBIT_AMQP,
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
