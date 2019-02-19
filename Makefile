.PHONY: test unit_tests integration_tests

build: install

install:
	pipenv install --dev

test:
	pipenv check
	pipenv run flake8 .
	pipenv run pytest test/
