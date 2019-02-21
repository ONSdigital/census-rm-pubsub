#!/bin/sh

sleep 10s

echo 'Running setup_pubsub.sh'
curl -X PUT http://localhost:8538/v1/projects/project/topics/eq-submission-topic
curl -X PUT http://localhost:8538/v1/projects/project/subscriptions/rm-receipt-subscription -H 'Content-Type: application/json' -d '{"topic": "projects/project/topics/eq-submission-topic"}'
