import sys

from google.cloud import pubsub_v1


if __name__ == '__main__':
    publisher = pubsub_v1.PublisherClient()
    try:
        topic_path = publisher.topic_path(sys.argv[1], sys.argv[2])
    except IndexError:
        print('Usage: python create_topic.py PROJECT_ID TOPIC_ID')
        sys.exit()

    topic = publisher.create_topic(topic_path)

    print(f'Topic created: {topic}')
