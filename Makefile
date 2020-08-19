#!/usr/bin/make -f

DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml -f docker-compose.ci.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

RUN_DEV = $(DOCKER_COMPOSE) run --rm bigquery-views-manager

GOOGLE_CLOUD_PROJECT = bigquery-views-manager

BIGQUERY_VIEWS_MANAGER_CLI_DOCKER = $(RUN_DEV) python -m bigquery_views_manager.cli
BIGQUERY_VIEWS_MANAGER_CLI_VENV = GOOGLE_CLOUD_PROJECT=$(GOOGLE_CLOUD_PROJECT) \
	$(PYTHON) -m bigquery_views_manager.cli
BIGQUERY_VIEWS_MANAGER_CLI = $(BIGQUERY_VIEWS_MANAGER_CLI_DOCKER)

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


dev-example-data-clean-dataset-config-data:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-clean-dataset-config-data


dev-example-data-clean-dataset-views:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-clean-dataset-views


dev-example-data-clean-dataset-materialized-tables:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-clean-dataset-materialized-tables


dev-example-data-clean-dataset:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-clean-dataset


dev-example-data-update-dataset-config-data:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-update-dataset-config-data


dev-example-data-update-dataset-views-and-materialize:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-update-dataset-views-and-materialize


dev-example-data-materialize-views:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-materialize-views


dev-example-data-diff-views:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-diff-views


dev-example-data-get-view:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-get-view


dev-example-data-get-all-views:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-get-all-views


dev-example-data-sort-view-list:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-sort-view-list


dev-example-data-update-dataset:
	$(MAKE) BIGQUERY_VIEWS_MANAGER_CLI="$(BIGQUERY_VIEWS_MANAGER_CLI_VENV)" \
		example-data-update-dataset


.PHONY: build
build:
	$(DOCKER_COMPOSE) build venv bigquery-views-manager


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
		delete-config-tables \
		--dataset=$(DATASET_NAME) \
		--config-tables-base-dir=./example-data/config-tables


example-data-clean-dataset-views: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		delete-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml


example-data-clean-dataset-materialized-tables: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		delete-materialized-tables \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml


example-data-clean-dataset: \
	example-data-clean-dataset-views \
	example-data-clean-dataset-materialized-tables \
	example-data-clean-dataset-config-data


example-data-update-dataset-config-data: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		create-or-replace-config-tables \
		--dataset=$(DATASET_NAME) \
		--config-tables-base-dir=./example-data/config-tables \
		$(ARGS)


example-data-update-dataset-views-and-materialize: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		create-or-replace-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		--materialize \
		$(ARGS)


example-data-materialize-views: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		materialize-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		$(ARGS)


example-data-diff-views: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		diff-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		$(ARGS)


example-data-get-view: .require-DATASET_NAME .require-VIEW_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		get-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		$(VIEW_NAME) \
		$(ARGS)


example-data-get-all-views: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		get-views \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		$(ARGS)


example-data-sort-view-list: .require-DATASET_NAME
	$(BIGQUERY_VIEWS_MANAGER_CLI) \
		sort-view-list \
		--dataset=$(DATASET_NAME) \
		--view-list-config=./example-data/views/views.yml \
		$(ARGS)


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
		bigquery-views-manager \
		./docker/push-testpypi-commit-version.sh "$(COMMIT)"


ci-push-pypi: .require-VERSION
	$(DOCKER_COMPOSE_CI) run --rm \
		-v $$PWD/.pypirc:/root/.pypirc \
		bigquery-views-manager \
		./docker/push-pypi-version.sh "$(VERSION)"


ci-clean:
	docker-compose down -v
