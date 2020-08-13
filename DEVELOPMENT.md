# BigQuery Views Manager (Development)

You can choose to use a virtual environment or Docker.

## Using virtual environment

### Pre-requisites (VENV)

* Make
* Python 3
* [Google Cloud SDK](https://cloud.google.com/sdk/docs/) for [gcloud](https://cloud.google.com/sdk/gcloud/)

## Build (VENV)

```bash
make dev-venv
```

## Test (VENV)

```bash
make dev-test
```

## Docker (CI)

### Pre-requisites (Docker)

* Make
* [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/)
* [Google Cloud SDK](https://cloud.google.com/sdk/docs/) for [gcloud](https://cloud.google.com/sdk/gcloud/)

## Build (Docker)

```bash
make build-dev
```

## Test (Docker)

```bash
make test
```
