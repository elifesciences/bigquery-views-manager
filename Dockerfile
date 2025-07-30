ARG python_base_image_tag
FROM python:${python_base_image_tag}

ARG PROJECT_HOME=/opt/bigquery-views-manager
WORKDIR ${PROJECT_HOME}

RUN python3 -m venv /.venv
ENV VIRTUAL_ENV=/.venv PYTHONUSERBASE=/.venv PATH=/.venv/bin:$PATH

COPY requirements.build.txt ./
RUN pip install --disable-pip-version-check -r requirements.build.txt

COPY requirements.txt ./
RUN pip install --disable-pip-version-check -r requirements.txt

ARG install_dev
COPY requirements.dev.txt ./
RUN pip install --disable-pip-version-check -r requirements.dev.txt
RUN if [ "${install_dev}" = "y" ]; then \
    pip install --disable-pip-version-check -r requirements.dev.txt; \
fi

COPY *.sh *.py *.txt README.md pytest.ini .pylintrc .flake8 setup.cfg ./
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
