#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml -f docker-compose.ci.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

RUN_DEV = $(DOCKER_COMPOSE) run --rm bigquery-views

BIGQUERY_VIEWS_MANAGER_CLI = $(RUN_DEV) python -m bigquery_views_manager.cli

ARGS =


.require-%:
	@ if [ "${${*}}" = "" ]; then \
			echo "Environment variable $* not set"; \
			exit 1; \
	fi


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
	$(RUN_DEV) python -m bigquery_views_manager.cli \
		$(ARGS)


example-data-clean-dataset-config-data: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		--dataset=$(DATASET_NAME) \
		delete-config-tables \
		--config-tables-base-dir=/tmp/example-data/config-tables


example-data-clean-dataset-views: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		--dataset=$(DATASET_NAME) \
		delete-views \
		--view-list-file=/tmp/example-data/views/views.lst \
		--disable-view-name-mapping


example-data-clean-dataset-materialized-tables: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		--dataset=$(DATASET_NAME) \
		delete-materialized-tables \
		--materialized-view-list-file=/tmp/example-data/views/materialized-views.lst \
		--disable-view-name-mapping


example-data-clean-dataset: \
	example-data-clean-dataset-views \
	example-data-clean-dataset-materialized-tables \
	example-data-clean-dataset-config-data


example-data-update-dataset-config-data: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		--dataset=$(DATASET_NAME) \
		create-or-replace-config-tables \
		--config-tables-base-dir=/tmp/example-data/config-tables


example-data-update-dataset-views-and-materialize:
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		--dataset=$(DATASET_NAME) \
		create-or-replace-views \
		--view-list-file=/tmp/example-data/views/views.lst \
		--materialized-view-list-file=/tmp/example-data/views/materialized-views.lst \
		--materialize \
		--disable-view-name-mapping


example-data-update-dataset: \
	example-data-update-dataset-config-data \
	example-data-update-dataset-views-and-materialize


ci-build-and-test:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build \
		build-dev \
		test


ci-views-manager-cli:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		views-manager-cli


ci-example-data-clean-dataset: .require-DATASET_NAME
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		example-data-clean-dataset


ci-example-data-update-dataset: .require-DATASET_NAME
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		example-data-update-dataset


ci-push-testpypi: .require-COMMIT
	$(DOCKER_COMPOSE_CI) run --rm \
		-v $$PWD/.pypirc:/root/.pypirc \
		bigquery-views \
		./docker/push-testpypi-commit-version.sh "$(COMMIT)"


ci-push-pypi: .require-VERSION
	$(DOCKER_COMPOSE_CI) run --rm \
		-v $$PWD/.pypirc:/root/.pypirc \
		bigquery-views \
		./docker/push-pypi-version.sh "$(VERSION)"


ci-clean:
	docker-compose down -v
