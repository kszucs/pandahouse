default:

.PHONY: test
test:
	python3 -m pytest .

.PHONY: poetry-test
poetry-test:
	poetry run make test

.PHONY: build
build: check
	poetry build -f wheel

check:
	poetry run black --check pandahouse
	flake8 pandahouse
