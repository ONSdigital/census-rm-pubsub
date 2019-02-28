#!/bin/sh

sleep 20s

source .env

echo 'Running setup_pubsub.sh'
curl -X PUT http://localhost:8539/v1/projects/$RECEIPT_TOPIC_PROJECT_ID/topics/$RECEIPT_TOPIC_NAME
curl -X PUT http://localhost:8539/v1/projects/$RECEIPT_TOPIC_PROJECT_ID/subscriptions/$SUBSCRIPTION_NAME -H 'Content-Type: application/json' -d '{"topic": "projects/'$RECEIPT_TOPIC_PROJECT_ID'/topics/'$RECEIPT_TOPIC_NAME'"}'
