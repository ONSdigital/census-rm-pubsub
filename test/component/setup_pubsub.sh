#!/bin/sh

#These sleeps will be removed in next version that polls end points or readiness file
echo 'Sleeping for 20 seconds for emulator and rabbitmq to be up'
sleep 20s
echo 'Running setup_pubsub.sh'
curl -X PUT http://localhost:8538/v1/projects/project/topics/eq-submission-topic
curl -X PUT http://localhost:8538/v1/projects/project/subscriptions/rm-receipt-subscription -H 'Content-Type: application/json' -d '{"topic": "projects/project/topics/eq-submission-topic"}'

echo 'Sleeping for 20 seconds for census pubsub to up'
sleep 20s