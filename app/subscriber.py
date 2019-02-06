import json
import logging

import jinja2
from google.api_core.exceptions import AlreadyExists
from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from rfc3339 import parse_datetime

from structlog import wrap_logger

from app.rabbit_helper import send_message_to_rabbitmq


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
            logger.error('Unknown message eventType', eventType=message.attributes['eventType'])
            return
        bucket_name, object_name = message.attributes['bucketId'], message.attributes['objectId']
        logger.debug('Message received for processing',
                     bucket_name=bucket_name,
                     message_id=message.message_id,
                     object_name=object_name)
        payload = json.loads(message.data)
        time_obj_created = parse_datetime(payload['timeCreated']).isoformat()
    except KeyError:
        logger.exception('Message missing attribute')
        return
    xml_message = jinja_template.render(case_id=object_name,        # TODO: This assumes caseId is filename
                                        inbound_channel='OFFLINE',  # TODO: Hardcoded to OFFLINE for all response types
                                        response_datetime=time_obj_created)
    send_message_to_rabbitmq(xml_message)
    message.ack()


def setup_subscription(project_id, subscription_name, topic_name, callback=receipt_to_case):
    """
    Create (it not exists) a new pubsub subscription in GCP to a pubsub topic
    and a subscriber thread which handles new messages through a callback

    :param project_id: GCP project identifier
    :param subscription_name: The name of the pubsub subscription
    :param topic_name: The pubsub topic to subscribe to
    :param callback: The callback to use upon receipt of a new message from the subscription
    :return: a StreamingPullFuture for managing the callback thread
    """
    topic_path = client.topic_path(project_id, topic_name)
    subscription_path = client.subscription_path(project_id, subscription_name)
    try:
        client.create_subscription(subscription_path, topic_path)
    except AlreadyExists:
        logger.info('Subscription already exists', subscription_path=subscription_path, topic_path=topic_path)
    else:
        logger.info('Subscription created', subscription_path=subscription_path, topic_path=topic_path)
    subscriber_future = client.subscribe(subscription_path, callback)
    logger.info('Listening for messages', subscription_path=subscription_path, topic_path=topic_path)
    return subscriber_future
