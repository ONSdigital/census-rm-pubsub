import os
from unittest import TestCase
from unittest.mock import patch, MagicMock

from test.helpers.when_then_return import create_when_then_return


RECEIPT_TOPIC_NAME = 'test-topic'
RECEIPT_TOPIC_PROJECT_ID = 'test-project'
SUBSCRIPTION_NAME = 'test-subscription'
SUBSCRIPTION_PROJECT_ID = 'test-project-id'


class TestSubscriber(TestCase):
    gcp_bucket = 'test-bucket'
    gcp_object_id = 'test-object'
    subscriber_future = 'test-future'
    subscription_path = 'test-subscription-path'
    test_data = 'test-data'
    topic_path = 'test-topic-path'

    def setUp(self):
        test_environment_variables = {
            'RECEIPT_TOPIC_NAME': RECEIPT_TOPIC_NAME,
            'RECEIPT_TOPIC_PROJECT_ID': RECEIPT_TOPIC_PROJECT_ID,
            'SUBSCRIPTION_NAME': SUBSCRIPTION_NAME,
            'SUBSCRIPTION_PROJECT_ID': SUBSCRIPTION_PROJECT_ID,
        }
        os.environ.update(test_environment_variables)

    def test_subscription_set_up(self):
        from app.subscriber import setup_subscription

        with patch('app.subscriber.client') as mock_client:
            callback_func = create_when_then_return(None, return_value=None)

            mock_client.topic_path = create_when_then_return(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME,
                                                             return_value=self.topic_path)
            mock_client.subscription_path = create_when_then_return(SUBSCRIPTION_PROJECT_ID, SUBSCRIPTION_NAME,
                                                                    return_value=self.subscription_path)
            mock_client.create_subscription = create_when_then_return(self.subscription_path, self.topic_path,
                                                                      return_value=None)
            mock_client.subscribe = create_when_then_return(self.subscription_path, callback_func,
                                                            return_value=self.subscriber_future)

            actual_future = setup_subscription(subscription_name=SUBSCRIPTION_NAME,
                                               subscription_project_id=SUBSCRIPTION_PROJECT_ID,
                                               topic_name=RECEIPT_TOPIC_NAME,
                                               topic_project_id=RECEIPT_TOPIC_PROJECT_ID,
                                               callback=callback_func)

        assert actual_future == self.subscriber_future

    def test_receipt_to_case(self):
        from app.subscriber import receipt_to_case

        with patch('app.subscriber.send_message_to_rabbitmq') as mock_send_message_to_rabbit_mq:
            mock_message = MagicMock()
            mock_message.attributes = {'eventType': 'OBJECT_FINALIZE',
                                       'bucketId': self.gcp_bucket,
                                       'objectId': self.gcp_object_id}
            mock_message.data = self.test_data

            with patch('app.subscriber.json') as mock_json:
                payload = {'timeCreated': '2018-08-24T00:00:00Z'}
                mock_json.loads = create_when_then_return(self.test_data, return_value=payload)

                receipt_to_case(mock_message)

                expected_msg = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' \
                               '<ns2:caseReceipt xmlns:ns2="http://ons.gov.uk/ctp/response/casesvc/message/feedback">' \
                               '<caseId>test-object</caseId><inboundChannel>OFFLINE</inboundChannel>' \
                               '<responseDateTime>2018-08-24T00:00:00+00:00</responseDateTime></ns2:caseReceipt>'

                mock_send_message_to_rabbit_mq.assert_called_once_with(expected_msg)
                mock_message.ack.assert_called_once()

    def test_receipt_to_case_key_error(self):
        from app.subscriber import receipt_to_case

        with patch('app.subscriber.send_message_to_rabbitmq') as mock_send_message_to_rabbit_mq:
            mock_message = MagicMock()
            mock_message.attributes = {'TypoMissingKeyError': 'OBJECT_FINALIZE',
                                       'bucketId': self.gcp_bucket,
                                       'objectId': self.gcp_object_id}
            mock_message.data = self.test_data

            with patch('app.subscriber.json') as json_mock:
                payload = {'timeCreated': '2018-08-24T00:00:00Z'}
                json_mock.loads = create_when_then_return(self.test_data, return_value=payload)

                receipt_to_case(mock_message)

                mock_send_message_to_rabbit_mq.assert_not_called()
                mock_message.ack.assert_not_called()
