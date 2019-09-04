import json
import os
import uuid
from contextlib import contextmanager
from unittest import TestCase
from unittest.mock import patch, MagicMock

from test import create_stub_function


class TestSubscriber(TestCase):
    subscription_name = 'test-subscription'
    offline_subscription_name = 'test-offline-subscription'
    subscription_project_id = 'test-project-id'
    offline_subscription_project_id = 'test-offline-project-id'
    ppo_undelivered_subscription_name = 'test_ppo_undelivered_subscription'
    ppo_undelivered_subscription_project_id = 'test-ppo-undelivered-project-id'
    qm_undelivered_subscription_name = 'test_qm_undelivered_subscription'
    qm_undelivered_subscription_project_id = 'test-qm-undelivered-project-id'
    case_ref = 12345
    product_code = 'XYZ'
    case_id = 'e079cea4-1447-4529-aa70-8757f1806f60'
    questionnaire_id = '0120000000001000'
    created = '2008-08-24T00:00:00Z'
    parsed_created = '2008-08-24T00:00:00+00:00'
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
        missing_keys = set()
        for record in watcher.records:
            message_json = json.loads(record.message)
            try:
                if (
                        event in message_json.get('event', '')
                        and all(message_json[key] == val for key, val in kwargs.items())
                ):
                    break
            except KeyError as e:
                missing_keys.add(e.args[0])
        else:
            self.fail(('No matching log records present', event, missing_keys))

    @contextmanager
    def checkExpectedLogLine(self, expected_log_level, expected_log_event, expected_log_kwargs):
        """
        Wraps the assertLogs and assertLogLine methods into a single context manager.

        Example below:
        ```
        with self.checkExpectedLogLine('INFO', 'did something', {"pub": "sub"}):
            function_to_test()
        ```

        Equivalent to:
        ```
        with self.assertLogs('app', 'INFO') as cm:
            function_to_test()
        self.assertLogLine(cm, 'did something', pub='sub')
        ```

        :param expected_log_level: logging level to use in TestCase.assertLogs
        :param expected_log_event: log event string to check for in records
        :param expected_log_kwargs: log kwargs to check for in structlog records
        """
        from app.app_logging import logger_initial_config

        logger_initial_config()

        with self.assertLogs('app.subscriber', expected_log_level) as cm:
            try:
                yield cm
            finally:
                self.assertLogLine(cm, expected_log_event, **expected_log_kwargs)

    def setUp(self):
        test_environment_variables = {
            'SUBSCRIPTION_NAME': self.subscription_name,
            'OFFLINE_SUBSCRIPTION_NAME': self.offline_subscription_name,
            'SUBSCRIPTION_PROJECT_ID': self.subscription_project_id,
            'OFFLINE_SUBSCRIPTION_PROJECT_ID': self.offline_subscription_project_id,
            'PPO_UNDELIVERED_SUBSCRIPTION_NAME': self.ppo_undelivered_subscription_name,
            'PPO_UNDELIVERED_SUBSCRIPTION_PROJECT_ID': self.ppo_undelivered_subscription_project_id,
            'QM_UNDELIVERED_SUBSCRIPTION_NAME': self.qm_undelivered_subscription_name,
            'QM_UNDELIVERED_SUBSCRIPTION_PROJECT_ID': self.qm_undelivered_subscription_project_id
        }
        os.environ.update(test_environment_variables)

    def test_subscription_set_up(self):
        from app.subscriber import setup_subscription

        with patch('app.subscriber.client') as mock_client:
            callback_func = create_stub_function(return_value=None)

            mock_client.subscription_path = create_stub_function(self.subscription_project_id, self.subscription_name,
                                                                 return_value=self.subscription_path)
            mock_client.subscribe = create_stub_function(self.subscription_path, callback_func,
                                                         return_value=self.subscriber_future)

            actual_future = setup_subscription(subscription_name=self.subscription_name,
                                               subscription_project_id=self.subscription_project_id,
                                               callback=callback_func)

        assert actual_future == self.subscriber_future

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps(
            {"metadata": {"tx_id": "1", "questionnaire_id": self.questionnaire_id, "case_id": self.case_id},
             "timeCreated": self.created})
        mock_message.message_id = str(uuid.uuid4())

        create_stub_function(self.created, return_value=self.parsed_created)

        expected_log_event = 'Message processing complete'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'questionnaire_id': self.questionnaire_id,
            'created': self.parsed_created,
            'tx_id': '1',
            'case_id': self.case_id,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id
        }

        expected_rabbit_message = json.dumps(
            {'event': {
                'type': 'RESPONSE_RECEIVED',
                'source': 'RECEIPT_SERVICE',
                'channel': 'EQ',
                'dateTime': '2008-08-24T00:00:00+00:00',
                'transactionId': '1'
            },
                'payload': {
                    'response': {
                        'caseId': self.case_id,
                        'questionnaireId': self.questionnaire_id,
                        'unreceipt': False
                    }
                }
            })

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('INFO', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_called_once_with(expected_rabbit_message)
        mock_message.ack.assert_called_once()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_offline_receipt_to_case(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.data = json.dumps(
            {"transactionId": "1", "questionnaireId": self.questionnaire_id, "dateTime": self.created,
             "channel": "PQRS"})
        mock_message.message_id = str(uuid.uuid4())

        create_stub_function(self.created, return_value=self.parsed_created)

        expected_log_event = 'Message processing complete'
        expected_log_kwargs = {
            'questionnaire_id': self.questionnaire_id,
            'created': self.parsed_created,
            'tx_id': '1',
            'channel': 'PQRS',
            'subscription_name': self.offline_subscription_name,
            'subscription_project': self.offline_subscription_project_id,
            'message_id': mock_message.message_id
        }

        expected_rabbit_message = json.dumps(
            {'event': {
                'type': 'RESPONSE_RECEIVED',
                'source': 'RECEIPT_SERVICE',
                'channel': 'PQRS',
                'dateTime': '2008-08-24T00:00:00+00:00',
                'transactionId': '1'
            },
                'payload': {
                    'response': {
                        'questionnaireId': self.questionnaire_id,
                        'unreceipt': False
                    }
                }
            })

        from app.subscriber import offline_receipt_to_case

        with self.checkExpectedLogLine('INFO', expected_log_event, expected_log_kwargs):
            offline_receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_called_once_with(expected_rabbit_message)
        mock_message.ack.assert_called_once()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_ppo_undelivered_mail_to_case(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.data = json.dumps(
            {"dateTime": self.created,
             "caseRef": self.case_ref,
             "productCode": self.product_code,
             "channel": "PPO",
             "type": "UNDELIVERED_MAIL_REPORTED"})

        mock_message.message_id = str(uuid.uuid4())

        create_stub_function(self.created, return_value=self.parsed_created)

        expected_log_event = 'Message processing complete'
        expected_log_kwargs = {
            'case_ref': self.case_ref,
            'created': self.created,
            'product_code': self.product_code,
            'subscription_name': self.ppo_undelivered_subscription_name,
            'subscription_project': self.ppo_undelivered_subscription_project_id,
            'message_id': mock_message.message_id
        }

        expected_rabbit_message = json.dumps(
            {'event': {
                'type': 'UNDELIVERED_MAIL_REPORTED',
                'source': 'RECEIPT_SERVICE',
                'channel': 'PPO',
                'dateTime': '2008-08-24T00:00:00Z',
                'transactionId': '12345'
            },
                'payload': {
                    'fulfilmentInformation': {
                        'caseRef': self.case_ref,
                        'productCode': self.product_code
                    }
                }
            })
        from app.subscriber import ppo_undelivered_mail_to_case
        with patch('uuid.uuid4') as mock_uuid:
            with self.checkExpectedLogLine('DEBUG', expected_log_event, expected_log_kwargs):
                mock_uuid.return_value = '12345'
                ppo_undelivered_mail_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_called_once_with(expected_rabbit_message,
                                                               routing_key='event.fulfilment.undelivered')
        mock_message.ack.assert_called_once()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_qm_undelivered_mail_to_case(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.data = json.dumps(
            {"dateTime": self.created,
             "questionnaireId": self.questionnaire_id})
        mock_message.message_id = str(uuid.uuid4())

        create_stub_function(self.created, return_value=self.parsed_created)

        expected_log_event = 'Message processing complete'
        expected_log_kwargs = {
            'questionnaire_id': self.questionnaire_id,
            'created': self.created,
            'subscription_name': self.qm_undelivered_subscription_name,
            'subscription_project': self.qm_undelivered_subscription_project_id,
            'message_id': mock_message.message_id
        }

        expected_rabbit_message = json.dumps({
            'event': {
                'type': 'UNDELIVERED_MAIL_REPORTED',
                'source': 'RECEIPT_SERVICE',
                'channel': 'QM',
                'dateTime': '2008-08-24T00:00:00Z',
                'transactionId': '12345'
            },
            'payload': {
                'fulfilmentInformation': {
                    'questionnaireId': self.questionnaire_id
                }
            }
        })
        from app.subscriber import qm_undelivered_mail_to_case
        with patch('uuid.uuid4') as mock_uuid:
            with self.checkExpectedLogLine('DEBUG', expected_log_event, expected_log_kwargs):
                mock_uuid.return_value = '12345'
                qm_undelivered_mail_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_called_once_with(expected_rabbit_message,
                                                               routing_key='event.fulfilment.undelivered')
        mock_message.ack.assert_called_once()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_eventType(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'bucketId': self.gcp_bucket,
            'objectId': self.gcp_object_id}

        expected_log_event = 'Pub/Sub Message missing required attribute'
        expected_log_kwargs = {
            'missing_attribute': 'eventType',
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_bucketId(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'eventType': 'OBJECT_FINALIZE',
            'objectId': self.gcp_object_id}

        expected_log_event = 'Pub/Sub Message missing required attribute'
        expected_log_kwargs = {
            'missing_attribute': 'bucketId',
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_objectId(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {
            'eventType': 'OBJECT_FINALIZE',
            'bucketId': self.gcp_bucket}

        expected_log_event = 'Pub/Sub Message missing required attribute'
        expected_log_kwargs = {
            'missing_attribute': 'objectId',
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_bad_eventType(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'FAIL'}

        expected_log_event = 'Unknown Pub/Sub Message eventType'
        expected_log_kwargs = {
            'eventType': 'FAIL',
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_json_data(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = None

        expected_log_event = 'Pub/Sub Message data not JSON'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_json_metadata(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({})

        expected_log_event = 'Pub/Sub Message missing required data'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
            'missing_json_key': 'metadata',
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_json_metadata_questionnaire_id(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"tx_id": "1", "timeCreated": ""}})

        expected_log_event = 'Pub/Sub Message missing required data'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
            'missing_json_key': 'questionnaire_id',
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_json_metadata_tx_id(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"case_id": "1", "timeCreated": ""}})

        expected_log_event = 'Pub/Sub Message missing required data'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
            'missing_json_key': 'tx_id',
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_missing_json_metadata_timeCreated(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps({"metadata": {"tx_id": "1", "questionnaire_id": "0120000000001000"}})

        expected_log_event = 'Pub/Sub Message missing required data'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
            'missing_json_key': 'timeCreated',
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()

    @patch('app.subscriber.send_message_to_rabbitmq')
    def test_receipt_to_case_timeCreated_valueerror(self, mock_send_message_to_rabbit_mq):
        mock_message = MagicMock()
        mock_message.message_id = str(uuid.uuid4())
        mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                   'bucketId': self.gcp_bucket,
                                   'objectId': self.gcp_object_id}
        mock_message.data = json.dumps(
            {"metadata": {"tx_id": "1", "questionnaire_id": "0120000000001000"}, "timeCreated": "123"})

        expected_log_event = 'Pub/Sub Message has invalid RFC 3339 timeCreated datetime string'
        expected_log_kwargs = {
            'bucket_name': self.gcp_bucket,
            'object_name': self.gcp_object_id,
            'subscription_name': self.subscription_name,
            'subscription_project': self.subscription_project_id,
            'message_id': mock_message.message_id,
        }

        from app.subscriber import receipt_to_case

        with self.checkExpectedLogLine('ERROR', expected_log_event, expected_log_kwargs):
            receipt_to_case(mock_message)

        mock_send_message_to_rabbit_mq.assert_not_called()
        mock_message.ack.assert_not_called()
