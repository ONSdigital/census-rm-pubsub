import json
import logging
import os

import jinja2
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from rfc3339 import parse_datetime

from structlog import wrap_logger

from app.rabbit_helper import send_message_to_rabbitmq


SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "rm-receipt-subscription")
SUBSCRIPTION_PROJECT_ID = os.getenv("SUBSCRIPTION_PROJECT_ID")

logger = wrap_logger(logging.getLogger(__name__))
client = SubscriberClient()

env = jinja2.Environment(loader=jinja2.FileSystemLoader(["app/templates"]))
jinja_template = env.get_template("message_template.xml")


def receipt_to_case(message: Message):
    """
    Callback for handling new pubsub messages which attempts to publish a receipt to the case service

    NB: any exceptions raised by this callback should nack the message by the future manager
    :param message: a GCP pubsub subscriber Message
    """
    try:
        if message.attributes['eventType'] != 'OBJECT_FINALIZE':  # receipt only on object creation
            logger.error('Unknown Pub/Sub Message eventType', eventType=message.attributes['eventType'])
            return
        bucket_name, object_name = message.attributes['bucketId'], message.attributes['objectId']
        logger.info('Pub/Sub Message received for processing',
                    bucket_name=bucket_name,
                    message_id=message.message_id,
                    object_name=object_name)
        payload = json.loads(message.data)
        time_obj_created = parse_datetime(payload['timeCreated']).isoformat()
    except KeyError:
        logger.exception('Pub/Sub Message missing attribute')
        return
    xml_message = jinja_template.render(case_id=object_name,        # TODO: This assumes caseId is filename
                                        inbound_channel='OFFLINE',  # TODO: Hardcoded to OFFLINE for all response types
                                        response_datetime=time_obj_created)
    send_message_to_rabbitmq(xml_message)
    message.ack()


def setup_subscription(subscription_name=SUBSCRIPTION_NAME,
                       subscription_project_id=SUBSCRIPTION_PROJECT_ID,
                       callback=receipt_to_case):
    """
    Create a subscriber thread which handles new messages through a callback

    :param subscription_name: The name of the pubsub subscription
    :param subscription_project_id: GCP project where subscription should already exist
    :param callback: The callback to use upon receipt of a new message from the subscription
    :return: a StreamingPullFuture for managing the callback thread
    """
    subscription_path = client.subscription_path(subscription_project_id, subscription_name)
    subscriber_future = client.subscribe(subscription_path, callback)
    logger.info('Listening for Pub/Sub messages', subscription_path=subscription_path)
    return subscriber_future
