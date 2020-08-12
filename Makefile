#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

RUN_DEV = $(DOCKER_COMPOSE) run --rm bigquery-views

ARGS =


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


venv-link:
	ln -s $(VENV) venv


dev-install:
	$(PIP) install --disable-pip-version-check -r requirements.build.txt
	$(PIP) install --disable-pip-version-check -r requirements.txt
	$(PIP) install --disable-pip-version-check -r requirements.dev.txt


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 bigquery_views_manager tests


dev-pylint:
	$(PYTHON) -m pylint bigquery_views_manager tests


dev-lint: dev-flake8 dev-pylint


dev-pytest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS)


dev-watch:
	$(PYTHON) -m pytest_watch --verbose -- -p no:cacheprovider -k 'not slow' $(ARGS)


dev-watch-slow:
	$(PYTHON) -m pytest_watch --verbose -- -p no:cacheprovider $(ARGS)


dev-test: dev-lint dev-pytest


.PHONY: build
build:
	$(DOCKER_COMPOSE) build venv bigquery-views


build-dev: build
	# currently there is no separate dev image


flake8:
	$(RUN_DEV) flake8 bigquery_views_manager tests


pylint:
	$(RUN_DEV) pylint bigquery_views_manager tests


lint: flake8 pylint


pytest:
	$(RUN_DEV) pytest


test: lint pytest


views-manager-cli:
	$(RUN_DEV) python -m bigquery_views.cli \
		$(ARGS)


ci-build-and-test:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build \
		build-dev \
		test


ci-views-manager-cli:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		views-manager-cli


ci-clean:
	docker-compose down -v
