ARG venv_image
ARG python_base_image_tag
FROM ${venv_image} as venv
FROM python:${python_base_image_tag}

COPY --from=venv /.venv/ /.venv/
ENV PYTHONUSERBASE=/.venv PATH=/.venv/bin:$PATH

ARG PROJECT_HOME=/opt/bigquery-views-manager
WORKDIR ${PROJECT_HOME}

COPY *.sh *.py *.txt README.md pytest.ini .pylintrc .flake8 ./
COPY bigquery_views_manager bigquery_views_manager
RUN pip install -e . --no-dependencies

# tests
COPY tests tests

ARG version
ADD docker ./docker
RUN ls -l && ./docker/set-version.sh "${version}"
LABEL org.opencontainers.image.version=${version}

RUN mkdir -p /data
WORKDIR /data

ENTRYPOINT ["python", "-m", "bigquery_views_manager"]
