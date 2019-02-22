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
    log = logger.bind(message_id=message.message_id,
                      subscription_name=SUBSCRIPTION_NAME,
                      subscription_project=SUBSCRIPTION_PROJECT_ID)
    try:
        if message.attributes['eventType'] != 'OBJECT_FINALIZE':  # receipt only on object creation
            log.error('Unknown Pub/Sub Message eventType', eventType=message.attributes['eventType'])
            return
        bucket_name, object_name = message.attributes['bucketId'], message.attributes['objectId']
    except KeyError as e:
        log.error('Pub/Sub Message missing required attribute(s)', missing_attribute=e.args[0])
        return
    else:
        log = log.bind(bucket_name=bucket_name, object_name=object_name)
        log.info('Pub/Sub Message received for processing')
    try:
        payload = json.loads(message.data)  # parse metadata as JSON payload
    except json.JSONDecodeError:
        log.error('Pub/Sub Message data not JSON')
        return
    try:
        metadata = payload['metadata']
        case_id, tx_id, created = metadata['case_id'], metadata['tx_id'], payload['timeCreated']
    except KeyError as e:
        log.error('Pub/Sub Message missing required data', missing_json_key=e.args[0])
        return
    else:
        log = log.bind(case_id=case_id, created=created, tx_id=tx_id)

    time_obj_created = parse_datetime(created).isoformat()

    xml_message = jinja_template.render(case_id=case_id,
                                        inbound_channel='OFFLINE',  # TODO: Hardcoded to OFFLINE for all response types
                                        response_datetime=time_obj_created)
    send_message_to_rabbitmq(xml_message)
    message.ack()

    log.info('Message processing complete')


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
    logger.info('Listening for Pub/Sub Messages', subscription_path=subscription_path)
    return subscriber_future
