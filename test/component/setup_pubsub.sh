#!/bin/sh

wait_for_curl_success() {
    echo "Waiting for: " "$3" " to be ready"

    while true; do
        response=$(curl -X $2 --write-out %{http_code} --silent --output /dev/null $1)

        if [[ response -eq 200 ]]; then
            echo "$3" " ready"
            break
        fi

        if [[ response -eq 409 ]]; then
            echo "$3" " already ready 409 means already exists? is this ok for testing, not 'pure'"
            break
        fi

        echo "$3" "not ready" "$response" "was the response"
        sleep 1s
    done
}
echo 'Running setup_pubsub.sh'

echo "Calling check for pubsub_emualator to be ready"
wait_for_curl_success "http://localhost:8538/v1/projects/project/topics/eq-submission-topic" "PUT" "pubsub_emulator topic"

echo 'setting up subscription to topic'
curl -X PUT http://localhost:8538/v1/projects/project/subscriptions/rm-receipt-subscription -H 'Content-Type: application/json' -d '{"topic": "projects/project/topics/eq-submission-topic"}'

echo "Calling check for rabbitmq to be be ready"
wait_for_curl_success "http://guest:guest@localhost:46672/api/aliveness-test/%2F" "GET" "rabbit_mq"

echo "Waiting for census pubsub to be ready"
while true; do
    docker cp pubsub:/app/pubsub-ready .

    if [[ ! -f pubsub-ready ]]; then
        echo "File not found! pubsub-ready"
        sleep 1s
        continue
    fi

    echo "census-pubsub ready"
    break
done

echo 'containers running and alive'