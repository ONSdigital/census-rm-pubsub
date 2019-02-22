#!/bin/sh

echo 'Running setup_pubsub.sh'
rm pubsub-ready

while true; do
    response=$(curl -X PUT --write-out %{http_code} --silent --output /dev/null http://localhost:8538/v1/projects/project/topics/eq-submission-topic)

    if [[ response -eq 200 ]]; then
        echo "Curl Result: " "$response"
        echo "Pubsub emulator ready"
        break
    fi

    if [[ response -eq 409 ]]; then
        echo "Curl Result: " "$response"
        echo "Pubsub emulator ready already ready? 409 means topic already exists? is this ok for testing, not 'pure'"
        break
    fi

    echo "Pubsub emulator not ready " "$response" " was the response"
    sleep 1s
done

curl -X PUT http://localhost:8538/v1/projects/project/subscriptions/rm-receipt-subscription -H 'Content-Type: application/json' -d '{"topic": "projects/project/topics/eq-submission-topic"}'

while true; do
    docker cp census-pubsub:/app/pubsub-ready .

    if [[ ! -f pubsub-ready ]]; then
        echo "File not found! pubsub-ready"
        sleep 1s
        continue
    fi

    echo "census-pubsub ready"
    break
done

