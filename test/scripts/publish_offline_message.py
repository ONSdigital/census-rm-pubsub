import json
import sys
import time

from google.api_core.exceptions import GoogleAPIError
from google.cloud import pubsub_v1

if __name__ == '__main__':
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(sys.argv[1], sys.argv[2])
    except IndexError:
        print('Usage: python publish_offline_message.py PROJECT_ID TOPIC_ID')
        sys.exit()

    data = json.dumps(
        {"dateTime": "2008-08-24T00:00:00Z", "productCode": "H1", "channel": "PQRS", "questionnaireId": "1100000000113",
         "source": "RECEIPT-SERVICE", "type": "FULFILMENT_CONFIRMED",
         "transactionId": "d34d68de-96df-431d-abe7-559b5c6aa325"})

    future = publisher.publish(topic_path,
                               data=data.encode('utf-8'))
    if not future.done():
        time.sleep(1)
    try:
        future.result(timeout=30)
    except GoogleAPIError:
        print("Failed to publish message to pubsub")

    print(f'Message published to {topic_path}')
