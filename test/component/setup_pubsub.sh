#!/bin/bash

echo 'Running setup_pubsub.sh'

wait_for_curl_success() {
    local healthcheck_url=${1}
    local http_verb=$2
    local service_name=${3}

    echo "Waiting for [$service_name] to be ready"

    while true; do
        response=$(curl -X $http_verb --write-out %{http_code} --silent --output /dev/null $healthcheck_url)

        if [[ response -eq 200 ]] || [[ response -eq 409 ]]; then
          break
        fi

        echo "[$service_name] not ready ([$response] was the response from curl)"
        sleep 1s
    done

	echo "[$service_name] is ready"
}

wait_for_curl_success "http://localhost:8538/v1/projects/project/topics/eq-submission-topic" "PUT" "pubsub_emulator topic"

echo "Setting up subscription to topic..."
curl -X PUT http://localhost:8538/v1/projects/project/subscriptions/rm-receipt-subscription -H 'Content-Type: application/json' -d '{"topic": "projects/project/topics/eq-submission-topic"}'

wait_for_curl_success "http://guest:guest@localhost:46672/api/aliveness-test/%2F" "GET" "rabbit_mq"

echo "Waiting for [pubsub] to be ready"
while true; do
    response=$(docker inspect pubsub -f "{{ .State.Health.Status }}")
    if [[ "$response" == "healthy" ]]; then
    	echo "[pubsub] is ready"
    	break
	fi

	echo "[pubsub] not ready ([$response] is its current state)"
	sleep 1s
done

echo "Containers running and alive"
