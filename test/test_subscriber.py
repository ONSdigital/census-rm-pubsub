from unittest.mock import patch
from app.subscriber import setup_subscription
from test.helpers.when_then_return import create_when_then_return

SUBSCRIPTION_PATH = 'test-subscription-path'
TOPIC_PATH = 'test-topic-path'
RECEIPT_TOPIC_NAME = 'test-topic'
RECEIPT_TOPIC_PROJECT_ID = 'test-project'
SUBSCRIPTION_NAME = 'test-subscription'
SUBSCRIPTION_PROJECT_ID = 'test-project-id'
SUBSCRIBER_FUTURE = 'test-future'


def test_subscription_set_up():
    with patch('app.subscriber.client') as mock_client:
        callback_func = create_when_then_return(None, return_value=None)

        mock_client.topic_path = create_when_then_return(RECEIPT_TOPIC_PROJECT_ID, RECEIPT_TOPIC_NAME,
                                                         return_value=TOPIC_PATH)
        mock_client.subscription_path = create_when_then_return(SUBSCRIPTION_PROJECT_ID, SUBSCRIPTION_NAME,
                                                                return_value=SUBSCRIPTION_PATH)
        mock_client.create_subscription = create_when_then_return(SUBSCRIPTION_PATH, TOPIC_PATH, return_value=None)
        mock_client.subscribe = create_when_then_return(SUBSCRIPTION_PATH, callback_func,
                                                        return_value=SUBSCRIBER_FUTURE)

        actual_future = setup_subscription(subscription_name=SUBSCRIPTION_NAME,
                                           subscription_project_id=SUBSCRIPTION_PROJECT_ID,
                                           topic_name=RECEIPT_TOPIC_NAME,
                                           topic_project_id=RECEIPT_TOPIC_PROJECT_ID,
                                           callback=callback_func)

    assert actual_future == SUBSCRIBER_FUTURE
