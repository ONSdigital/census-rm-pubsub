import json
import os
import uuid
from unittest import TestCase
from unittest.mock import patch, MagicMock

from test import create_stub_function


SUBSCRIPTION_NAME = 'test-subscription'
SUBSCRIPTION_PROJECT_ID = 'test-project-id'


class TestSubscriber(TestCase):
    case_id = 'e079cea4-1447-4529-aa70-8757f1806f60'
    created = '2008-08-24T00:00:00Z'
    gcp_bucket = 'test-bucket'
    gcp_object_id = 'test-object'
    subscriber_future = 'test-future'
    subscription_path = 'test-subscription-path'
    test_data = 'test-data'

    def assertLogLine(self, watcher, event, **kwargs):
        """
        Helper method for asserting the contents of structlog records caught by self.assertLogs.
        Fails if no match is found. A match is based on the main log message (event) and all additional
        items passed in kwargs.
        :param watcher: context manager returned by `with self.assertLogs(LOGGER, LEVEL)`
        :param event: event logged; use empty string to ignore or no message
        :param kwargs: other structlog key value pairs to assert for
        """
        for record in watcher.records:
            message_json = json.loads(record.message)
            try:
                if (
                        event in message_json.get('event', '')
                        and all(message_json[key] == val for key, val in kwargs.items())
                ):
                    break
            except KeyError:
                pass
        else:
            self.fail(f'No matching log records present: {event}')

    def setUp(self):
        test_environment_variables = {
            'SUBSCRIPTION_NAME': SUBSCRIPTION_NAME,
            'SUBSCRIPTION_PROJECT_ID': SUBSCRIPTION_PROJECT_ID,
        }
        os.environ.update(test_environment_variables)

    def test_subscription_set_up(self):
        from app.subscriber import setup_subscription

        with patch('app.subscriber.client') as mock_client:
            callback_func = create_stub_function(return_value=None)

            mock_client.subscription_path = create_stub_function(SUBSCRIPTION_PROJECT_ID, SUBSCRIPTION_NAME,
                                                                 return_value=self.subscription_path)
            mock_client.subscribe = create_stub_function(self.subscription_path, callback_func,
                                                         return_value=self.subscriber_future)

            actual_future = setup_subscription(subscription_name=SUBSCRIPTION_NAME,
                                               subscription_project_id=SUBSCRIPTION_PROJECT_ID,
                                               callback=callback_func)

        assert actual_future == self.subscriber_future

    def test_receipt_to_case(self):
        mock_message = MagicMock()
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps(
            {"metadata": {"tx_id": "1", "case_id": self.case_id}, "timeCreated": self.created})
        mock_message.message_id = str(uuid.uuid4())

        expected_log_entry = {
            'event': 'Message processing complete',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'case_id': self.case_id,
                'created': self.created,
                'tx_id': '1',
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id
            },
            'level': 'INFO',
        }

        expected_rabbit_msg = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' \
                              '<ns2:caseReceipt xmlns:ns2="http://ons.gov.uk/ctp/response/casesvc/message/feedback">' \
                              f'<caseId>{self.case_id}</caseId>' \
                              '<inboundChannel>OFFLINE</inboundChannel>' \
                              '<responseDateTime>2008-08-24T00:00:00+00:00</responseDateTime></ns2:caseReceipt>'

        self._receipt_to_case_with_logging(mock_message, expected_log_entry, expected_rabbit_msg)

    def test_receipt_to_case_missing_eventType(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'bucketId': self.gcp_bucket,
            'objectId': self.gcp_object_id}

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required attribute',
            'kwargs': {
                'missing_attribute': 'eventType',
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_bucketId(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'eventType': 'OBJECT_FINALIZE',
            'objectId': self.gcp_object_id}

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required attribute',
            'kwargs': {
                'missing_attribute': 'bucketId',
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_objectId(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'eventType': 'OBJECT_FINALIZE',
            'bucketId': self.gcp_bucket}

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required attribute',
            'kwargs': {
                'missing_attribute': 'objectId',
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_bad_eventType(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'FAIL'}

        expected_log_entry = {
            'event': 'Unknown Pub/Sub Message eventType',
            'kwargs': {
                'eventType': 'FAIL',
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_json_data(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = None

        expected_log_entry = {
            'event': 'Pub/Sub Message data not JSON',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_json_metadata(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({})

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required data',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
                'missing_json_key': 'metadata',
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_json_metadata_case_id(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"tx_id": "1", "timeCreated": ""}})

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required data',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
                'missing_json_key': 'case_id',
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_json_metadata_tx_id(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"case_id": "1", "timeCreated": ""}})

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required data',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
                'missing_json_key': 'tx_id',
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    def test_receipt_to_case_missing_json_metadata_timeCreated(self):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"tx_id": "1", "case_id": "2"}})

        expected_log_entry = {
            'event': 'Pub/Sub Message missing required data',
            'kwargs': {
                'bucket_name': self.gcp_bucket,
                'object_name': self.gcp_object_id,
                'subscription_name': SUBSCRIPTION_NAME,
                'subscription_project': SUBSCRIPTION_PROJECT_ID,
                'message_id': mock_message.message_id,
                'missing_json_key': 'timeCreated',
            },
            'level': 'ERROR',
        }
        self._failed_receipt_to_case_with_logging(mock_message, expected_log_entry)

    @patch('app.subscriber.send_message_to_rabbitmq')
    def _failed_receipt_to_case_with_logging(self, message, expected_log_entry, mock_send_message_to_rabbit_mq):
        from app.app_logging import logger_initial_config
        from app.subscriber import receipt_to_case

        logger_initial_config()

        with self.assertLogs('app.subscriber', expected_log_entry['level']) as cm:
            receipt_to_case(message)

        self.assertLogLine(cm, expected_log_entry['event'], **expected_log_entry['kwargs'])

        mock_send_message_to_rabbit_mq.assert_not_called()
        message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def _receipt_to_case_with_logging(self, message, expected_log_entry, expected_rabbit_message,
                                      mock_send_message_to_rabbit_mq):
        from app.app_logging import logger_initial_config
        from app.subscriber import receipt_to_case

        logger_initial_config()

        with self.assertLogs('app.subscriber', expected_log_entry['level']) as cm:
            receipt_to_case(message)

        self.assertLogLine(cm, expected_log_entry['event'], **expected_log_entry['kwargs'])

        mock_send_message_to_rabbit_mq.assert_called_once_with(expected_rabbit_message)
        message.ack.assert_called_once()
