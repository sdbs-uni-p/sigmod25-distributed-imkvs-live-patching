.PHONY: all
all: install imports format lint type

PYTHON_EXEC=pipenv run

.PHONY: install
install:
	pipenv sync

.PHONY: imports
imports:
	${PYTHON_EXEC} pycln .
	${PYTHON_EXEC} isort .

.PHONY: format
format:
	${PYTHON_EXEC} black .

.PHONY: lint
lint:
	${PYTHON_EXEC} flake8 .

.PHONY: type
type:
	${PYTHON_EXEC} mypy .

