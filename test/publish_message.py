import sys
import time

from google.cloud import pubsub_v1


if __name__ == '__main__':
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(sys.argv[1], sys.argv[2])
    except IndexError:
        print('Usage: python publish_message.py PROJECT_ID TOPIC_ID')
        sys.exit()

    data = '{"timeCreated":"2008-08-24T00:00:00Z"}'.encode('utf-8')

    future = publisher.publish(topic_path,
                               data=data,
                               eventType='OBJECT_FINALIZE',
                               bucketId='123',
                               objectId='2')

    while not future.done():
        time.sleep(1)

    print(f'Message published to {topic_path}')
