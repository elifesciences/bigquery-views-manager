VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


venv-link:
	ln -s $(VENV) venv


dev-install:
	$(PIP) install -r requirements.build.txt
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements.dev.txt


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 bigquery_views tests view_tests


dev-pylint:
	$(PYTHON) -m pylint bigquery_views tests view_tests


dev-lint: dev-flake8 dev-pylint


dev-pytest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS)


dev-watch:
	$(PYTHON) -m pytest_watch --verbose -- -p no:cacheprovider -k 'not slow' $(ARGS)


dev-watch-slow:
	$(PYTHON) -m pytest_watch --verbose -- -p no:cacheprovider $(ARGS)


dev-test: dev-lint dev-pytest


ci-build-and-test:
	# TODO
	echo "build placeholder: IMAGE_TAG=$(IMAGE_TAG)"

ci-clean:
	# TODO
