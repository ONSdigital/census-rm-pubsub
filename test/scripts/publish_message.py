import json
import sys
import time
import uuid

from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1


if __name__ == '__main__':
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(sys.argv[1], sys.argv[2])
    except IndexError:
        print('Usage: python publish_message.py PROJECT_ID TOPIC_ID')
        sys.exit()

    tx_id = str(uuid.uuid4())
    case_id = str(uuid.uuid4())
    data = json.dumps({
        "timeCreated": "2008-08-24T00:00:00Z",
        "metadata": {
            "case_id": case_id,
            "tx_id": tx_id,
        }
    })

    future = publisher.publish(topic_path,
                               data=data.encode('utf-8'),
                               eventType='OBJECT_FINALIZE',
                               bucketId='123',
                               objectId=tx_id)
    if not future.done():
        time.sleep(1)
    try:
        future.result(timeout=30)
    except GoogleAPIError:
        print("Failed to publish message to pubsub")

    print(f'Message published to {topic_path}')
