# census-rm-pubsub

## Prerequisites

* An existing durable rabbitmq exchange (e.g.: `case-outbound-exchange`) that can be used to publish messages which get routed to the RM Case Service (e.g.: `Case.Responses.binding`).

* A GCS bucket with a [Cloud Pub/Sub notification configuration](https://cloud.google.com/storage/docs/reporting-changes):
	```bash
	gsutil notification create -t [TOPIC_NAME] -f json gs://[BUCKET_NAME]
	```

* Relevant environment variables:
	```bash
	RABBIT_AMQP=[RABBIT_URL]
	GCP_PROJECT_ID=[PROJECT_ID]
	RABBIT_QUEUE=[QUEUE_BINDING_ID]
	RABBIT_EXCHANGE=[EXCHANGE_ID]
	EQ_TOPIC_NAME=[TOPIC_NAME]
	```

* [Pipenv](https://docs.pipenv.org/index.html) for local development.

## To test receipting against RM (dev)

* Start RM services in Docker: 
```bash
git clone git@github.com:ONSdigital/ras-rm-docker-dev.git
pushd ras-rm-docker-dev
make up
popd
```

* POST to rm-sdx-gateway endpoint to create rabbitmq bindings: 
```bash

curl -X POST \
  http://0.0.0.0:8191/receipts \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Basic YWRtaW46c2VjcmV0' \
  -d '{"caseId": "e72b8990-960a-4be3-b14c-06600e38ee3d"}'
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

* Create .env file in census-rm-pubsub directory:
```bash
cat > .env << EOS
RABBIT_AMQP=amqp://guest:guest@localhost:6672
GCP_PROJECT_ID=project
PUBSUB_EMULATOR_HOST=localhost:8410
RABBIT_QUEUE=Case.Responses.binding
RABBIT_EXCHANGE=case-outbound-exchange
EQ_TOPIC_NAME=eq-submission-topic
EOS
```

* Run the census-rm-pubsub application:
```bash
pipenv install
pipenv shell

python test/create_topic.py $GCP_PROJECT_ID $EQ_TOPIC_NAME
python run.py
```

* In a separate terminal, tail the logs of the case service:
```bash
docker logs casesvc -f
```

* In a separate terminal, publish a message to the Pub/Sub emulator:
```bash
pipenv run python test/publish_message.py $GCP_PROJECT_ID $EQ_TOPIC_NAME
```