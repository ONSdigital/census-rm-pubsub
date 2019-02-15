# census-rm-pubsub 
[![Build Status](https://travis-ci.com/ONSdigital/census-rm-pubsub.svg?branch=master)](https://travis-ci.com/ONSdigital/census-rm-pubsub)

## Prerequisites

* An existing durable rabbitmq exchange (e.g.: `case-outbound-exchange`) that can be used to publish messages which get routed to the RM Case Service (e.g.: `Case.Responses.binding`).

* A GCS bucket with a [Cloud Pub/Sub notification configuration](https://cloud.google.com/storage/docs/reporting-changes).

* Relevant environment variables:
	```bash
	GOOGLE_APPLICATION_CREDENTIALS
	LOG_LEVEL
	RABBIT_AMQP
	RABBIT_QUEUE
	RABBIT_EXCHANGE
	RABBIT_ROUTE
	RECEIPT_TOPIC_NAME
	RECEIPT_TOPIC_PROJECT_ID
	SUBSCRIPTION_NAME
	SUBSCRIPTION_PROJECT_ID
	```

* [Pipenv](https://docs.pipenv.org/index.html) for local development.

# Testing

* [With an existing, accessible Google Cloud Project](#to-test-receipting-against-rm-with-gcp)
* [Without GCP (local emulation)](#to-test-receipting-against-rm-without-gcp)

## To test receipting against RM (with GCP)

* Create a GCS bucket with a Cloud Pub/Sub notification configuration:
```bash
gsutil mb -c regional -l europe-west2 -p [RECEIPT_TOPIC_PROJECT_ID] gs://[BUCKET_NAME]
gsutil notification create -t [TOPIC_NAME] -f json gs://[BUCKET_NAME]
```

* Start RM services in Docker:
```bash
git clone git@github.com:ONSdigital/ras-rm-docker-dev.git
cd ras-rm-docker-dev && make up
```

* Create `.env` file in census-rm-pubsub directory:
```bash
cat > .env << EOS
RABBIT_AMQP=amqp://guest:guest@localhost:6672
SUBSCRIPTION_PROJECT_ID=[SUB_PROJECT_ID]
RECEIPT_TOPIC_PROJECT_ID=[RECEIPT_TOPIC_PROJECT_ID]
GOOGLE_APPLICATION_CREDENTIALS=[/path/to/service/account/key.json]
RABBIT_QUEUE=Case.Responses
RABBIT_EXCHANGE=case-outbound-exchange
RABBIT_ROUTE=Case.Responses.binding
RECEIPT_TOPIC_NAME=[TOPIC_NAME]
SUBSCRIPTION_NAME=[NEW_OR_EXISTING_SUB_NAME]
EOS
```

* Run the census-rm-pubsub application:
```bash
pipenv run python run.py
```

* Upload a file to the `gs://[BUCKET_NAME]` bucket

## To test receipting against RM (without GCP)

* Start RM services in Docker:
```bash
git clone git@github.com:ONSdigital/ras-rm-docker-dev.git
cd ras-rm-docker-dev && make up
```

* Start Cloud Pub/Sub emulator:
```bash
gcloud components install pubsub-emulator
gcloud components update
gcloud beta emulators pubsub start
```

* Get Pub/Sub emulator-related environment variables:
```bash
gcloud beta emulators pubsub env-init
```
example output:
```
export PUBSUB_EMULATOR_HOST=::1:8410
```

* Create `.env` file in census-rm-pubsub directory:
```bash
cat > .env << EOS
RABBIT_AMQP=amqp://guest:guest@localhost:6672  # taken from ras-rm-docker-dev
SUBSCRIPTION_PROJECT_ID=project  # can be anything
RECEIPT_TOPIC_PROJECT_ID=project  # can be anything
PUBSUB_EMULATOR_HOST=localhost:8410  # taken from the env-init (above)
RABBIT_QUEUE=Case.Responses
RABBIT_EXCHANGE=case-outbound-exchange
RABBIT_ROUTE=Case.Responses.binding
RECEIPT_TOPIC_NAME=eq-submission-topic
SUBSCRIPTION_NAME=rm-receipt-subscription
EOS
```

* Run the census-rm-pubsub application:
```bash
pipenv install
pipenv shell

python test/create_topic.py $RECEIPT_TOPIC_PROJECT_ID $RECEIPT_TOPIC_NAME
python run.py
```

* In a separate terminal, tail the logs of the case service:
```bash
docker logs casesvc -f
```

* In a separate terminal, publish a message to the Pub/Sub emulator:
```bash
pipenv shell
python test/publish_message.py $RECEIPT_TOPIC_PROJECT_ID $RECEIPT_TOPIC_NAME
```