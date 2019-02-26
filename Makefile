DOT := $(shell command -v dot 2> /dev/null)

build: install

package_vulnerability:
	pipenv check

flake:
	pipenv run flake8 .

install:
	pipenv install --dev

test: package_vulnerability flake unit_tests component_tests

unit_tests:
	pipenv run pytest test/unit/

component_tests:
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