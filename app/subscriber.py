import json
import logging
import os
from datetime import datetime, timezone

from google.cloud.pubsub_v1 import SubscriberClient
from google.cloud.pubsub_v1.subscriber.message import Message
from rfc3339 import parse_datetime
from structlog import wrap_logger

from app.rabbit_helper import send_message_to_rabbitmq

SUBSCRIPTION_NAME = os.getenv("SUBSCRIPTION_NAME", "rm-receipt-subscription")
OFFLINE_SUBSCRIPTION_NAME = os.getenv("OFFLINE_SUBSCRIPTION_NAME", "rm-offline-receipt-subscription")
PPO_UNDELIVERED_SUBSCRIPTION_NAME = os.getenv("PPO_UNDELIVERED_SUBSCRIPTION_NAME", "rm-ppo-undelivered-subscription")
QM_UNDELIVERED_SUBSCRIPTION_NAME = os.getenv("QM_UNDELIVERED_SUBSCRIPTION_NAME", "rm-qm-undelivered-subscription")
SUBSCRIPTION_PROJECT_ID = os.getenv("SUBSCRIPTION_PROJECT_ID")
OFFLINE_SUBSCRIPTION_PROJECT_ID = os.getenv("OFFLINE_SUBSCRIPTION_PROJECT_ID")
PPO_UNDELIVERED_SUBSCRIPTION_PROJECT_ID = os.getenv("PPO_UNDELIVERED_SUBSCRIPTION_PROJECT_ID")
QM_UNDELIVERED_SUBSCRIPTION_PROJECT_ID = os.getenv("QM_UNDELIVERED_SUBSCRIPTION_PROJECT_ID")
UNDELIVERED_MAIL_ROUTING_KEY = os.getenv("UNDELIVERED_MAIL_ROUTING_KEY", "event.fulfilment.undelivered")

logger = wrap_logger(logging.getLogger(__name__))
client = SubscriberClient()


def eq_receipt_to_case(message: Message):
    """
    Callback for handling new pubsub messages which attempts to publish a receipt to the events exchange

    NB: any exceptions raised by this callback should nack the message by the future manager
    :param message: a GCP pubsub subscriber Message
    """
    log = logger.bind(message_id=message.message_id,
                      subscription_name=SUBSCRIPTION_NAME,
                      subscription_project=SUBSCRIPTION_PROJECT_ID)
    try:
        if message.attributes['eventType'] != 'OBJECT_FINALIZE':  # only forward on object creation
            log.error('Unknown Pub/Sub Message eventType', eventType=message.attributes['eventType'])
            return
        bucket_name, object_name = message.attributes['bucketId'], message.attributes['objectId']
    except KeyError as e:
        log.error('Pub/Sub Message missing required attribute', missing_attribute=e.args[0])
        return

    log = log.bind(bucket_name=bucket_name, object_name=object_name)
    log.info('Pub/Sub Message received for processing')

    payload = validate_eq_receipt(message.data, log, ['timeCreated'], ['tx_id', 'questionnaire_id'])
    if not payload:
        return  # Failed validation

    metadata = payload['metadata']
    tx_id, questionnaire_id, case_id = metadata['tx_id'], metadata['questionnaire_id'], metadata.get('case_id')
    time_obj_created = parse_datetime(payload['timeCreated']).isoformat()

    log = log.bind(questionnaire_id=questionnaire_id, created=time_obj_created, tx_id=tx_id, case_id=case_id)

    receipt_message = {
        'event': {
            'type': 'RESPONSE_RECEIVED',
            'source': 'RECEIPT_SERVICE',
            'channel': 'EQ',
            'dateTime': time_obj_created,
            'transactionId': tx_id
        },
        'payload': {
            'response': {
                'caseId': case_id,
                'questionnaireId': questionnaire_id,
                'unreceipt': False
            }
        }
    }

    send_message_to_rabbitmq(json.dumps(receipt_message))
    message.ack()

    log.info('Message processing complete')


def offline_receipt_to_case(message: Message):
    log = logger.bind(message_id=message.message_id,
                      subscription_name=OFFLINE_SUBSCRIPTION_NAME,
                      subscription_project=OFFLINE_SUBSCRIPTION_PROJECT_ID)

    log.info('Pub/Sub Message received for processing')

    payload = validate_offline_receipt(message.data, log, ['transactionId', 'questionnaireId', 'channel'])
    if not payload:
        return  # Failed validation

    tx_id, questionnaire_id, channel = payload['transactionId'], payload['questionnaireId'], payload['channel']
    time_obj_created = datetime.strptime(payload['dateTime'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).isoformat()

    log = log.bind(questionnaire_id=questionnaire_id, created=time_obj_created, tx_id=tx_id, channel=channel)

    receipt_message = {
        'event': {
            'type': 'RESPONSE_RECEIVED',
            'source': 'RECEIPT_SERVICE',
            'channel': channel,
            'dateTime': time_obj_created,
            'transactionId': tx_id
        },
        'payload': {
            'response': {
                'questionnaireId': questionnaire_id,
                'unreceipt': payload.get('unreceipt', False)
            }
        }
    }

    send_message_to_rabbitmq(json.dumps(receipt_message))
    message.ack()

    log.info('Message processing complete')


def ppo_undelivered_mail_to_case(message: Message):
    log = logger.bind(message_id=message.message_id,
                      subscription_name=PPO_UNDELIVERED_SUBSCRIPTION_NAME,
                      subscription_project=PPO_UNDELIVERED_SUBSCRIPTION_PROJECT_ID)

    log.debug('Pub/Sub Message received for processing')

    payload = validate_offline_receipt(message.data, log, ['transactionId', 'caseRef', 'productCode'])
    if not payload:
        return  # Failed validation

    tx_id, case_ref, product_code = payload['transactionId'], payload['caseRef'], payload['productCode']
    date_time = datetime.strptime(payload['dateTime'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).isoformat()

    log = log.bind(case_ref=case_ref, created=date_time, product_code=product_code, tx_id=tx_id)

    receipt_message = {
        'event': {
            'type': 'UNDELIVERED_MAIL_REPORTED',
            'source': 'RECEIPT_SERVICE',
            'channel': 'PPO',
            'dateTime': date_time,
            'transactionId': tx_id
        },
        'payload': {
            'fulfilmentInformation': {
                'caseRef': case_ref,
                'fulfilmentCode': product_code
            }
        }
    }

    send_message_to_rabbitmq(json.dumps(receipt_message), routing_key=UNDELIVERED_MAIL_ROUTING_KEY)
    message.ack()

    log.debug('Message processing complete')


def qm_undelivered_mail_to_case(message: Message):
    log = logger.bind(message_id=message.message_id,
                      subscription_name=QM_UNDELIVERED_SUBSCRIPTION_NAME,
                      subscription_project=QM_UNDELIVERED_SUBSCRIPTION_PROJECT_ID)

    log.debug('Pub/Sub Message received for processing')

    payload = validate_offline_receipt(message.data, log, ['transactionId', 'questionnaireId'])
    if not payload:
        return  # Failed validation

    tx_id, questionnaire_id = payload['transactionId'], payload['questionnaireId']
    date_time = datetime.strptime(payload['dateTime'], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).isoformat()

    log = log.bind(questionnaire_id=questionnaire_id, created=date_time, tx_id=tx_id)

    receipt_message = {
        'event': {
            'type': 'UNDELIVERED_MAIL_REPORTED',
            'source': 'RECEIPT_SERVICE',
            'channel': 'QM',
            'dateTime': date_time,
            'transactionId': tx_id
        },
        'payload': {
            'fulfilmentInformation': {
                'questionnaireId': questionnaire_id
            }
        }
    }

    send_message_to_rabbitmq(json.dumps(receipt_message), routing_key=UNDELIVERED_MAIL_ROUTING_KEY)
    message.ack()

    log.debug('Message processing complete')


def validate_offline_receipt(message_data, log, expected_keys, date_time_key='dateTime'):
    try:
        payload = json.loads(message_data)  # parse metadata as JSON payload
        for expected_key in expected_keys:
            if expected_key not in payload:
                log.error('Pub/Sub Message missing required data', missing_json_key=expected_key)
                return None

        datetime.strptime(payload[date_time_key], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).isoformat()

        return payload
    except (TypeError, json.JSONDecodeError):
        log.error('Pub/Sub Message data not JSON')
        return None
    except KeyError as e:
        log.error('Pub/Sub Message missing required data', missing_json_key=e.args[0])
        return None
    except ValueError:
        log.error('Pub/Sub Message has invalid datetime string')
        return None


def validate_eq_receipt(message_data, log, expected_keys, expected_metadata_keys, date_time_key='timeCreated'):
    try:
        payload = json.loads(message_data)  # parse metadata as JSON payload
        if 'metadata' not in payload:
            log.error('Pub/Sub Message missing required data', missing_json_key='metadata')
            return None

        for expected_key in expected_keys:
            if expected_key not in payload:
                log.error('Pub/Sub Message missing required data', missing_json_key=expected_key)
                return None

        for expected_metadata_key in expected_metadata_keys:
            if expected_metadata_key not in payload['metadata']:
                log.error('Pub/Sub Message missing required data', missing_json_key=expected_metadata_key)
                return None

        parse_datetime(payload[date_time_key]).isoformat()

        return payload
    except (TypeError, json.JSONDecodeError):
        log.error('Pub/Sub Message data not JSON')
        return None
    except KeyError as e:
        log.error('Pub/Sub Message missing required data', missing_json_key=e.args[0])
        return None
    except ValueError:
        log.error('Pub/Sub Message has invalid datetime string')
        return None


def setup_subscription(subscription_name=SUBSCRIPTION_NAME,
                       subscription_project_id=SUBSCRIPTION_PROJECT_ID,
                       callback=eq_receipt_to_case):
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
