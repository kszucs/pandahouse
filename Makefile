default:

.PHONY: test
test:
	python3 -m pytest .

.PHONY: poetry-test
poetry-test:
	poetry run make test

.PHONY: build
build:
	poetry build -f wheel

check:
	poetry run black --check pandahouse
