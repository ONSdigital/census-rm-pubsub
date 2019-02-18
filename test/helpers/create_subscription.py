import sys

from google.api_core.exceptions import AlreadyExists, PermissionDenied
from google.cloud import pubsub_v1


if __name__ == '__main__':
    subscriber = pubsub_v1.SubscriberClient()
    try:
        topic_path = subscriber.topic_path(sys.argv[1], sys.argv[2])
        subscription_path = subscriber.subscription_path(sys.argv[1], sys.argv[3])
    except IndexError:
        print('Usage: python create_subscription.py PROJECT_ID TOPIC_ID SUBSCRIPTION_ID')
        sys.exit()

    try:
        sub = subscriber.create_subscription(subscription_path, topic_path)
    except AlreadyExists:
        print('Subscription already exists')
    except PermissionDenied:
        print('Subscription can not be created')

    print(f'Subscription created: {sub}')
