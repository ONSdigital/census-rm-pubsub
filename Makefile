DOT := $(shell command -v dot 2> /dev/null)

.PHONY: test unit_tests integration_tests

build: install

install:
	pipenv install --dev

test: unit_tests component_tests
	pipenv check
	pipenv run flake8 .

unit_tests:
	pipenv run pytest test/unit/

component_tests: docker_build
	docker-compose up -d ;
	./test/component/setup_pubsub.sh
	pipenv run pytest test/component/

up: docker_build
	docker-compose up -d;
	./setup_pubsub.sh

down:
	docker-compose down

docker_build:
	docker build -t census-rm-pubsub:latest .