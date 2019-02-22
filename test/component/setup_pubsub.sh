#!/bin/sh

echo 'Running setup_pubsub.sh'

while true; do
    response=$(curl -X PUT --write-out %{http_code} --silent --output /dev/null http://localhost:8538/v1/projects/project/topics/eq-submission-topic)

#    could do this way
#    if [[ response -eq 200 ]] || [[ response -eq 409 ]]; then
#        echo "Curl Result: " "$response"
#        echo "Pubsub emulator ready"
#        break
#    fi
#
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

rf=$(docker diff census-pubsub | grep "A /app/pubsub-ready")

while true; do

    rf=$(docker diff census-pubsub | grep "A /app/pubsub-ready")

    echo ${rf}

    if [[ "$rf" == "A /app/pubsub-ready" ]]; then
        echo "Found /app/pubsub-ready file to indicate census-rm-pubsub is "
        break
    fi

    echo "/app/pubsub-ready file not found, census-rm-pubsub not yet ready"
    sleep 1s
done
