DOT := $(shell command -v dot 2> /dev/null)

.PHONY: test unit_tests integration_tests

build: install

install:
	pipenv install --dev

test:
	pipenv check
	pipenv run flake8 .
	pipenv run pytest test/

up:
	docker-compose up -d;
	./setup_pubsub.sh

