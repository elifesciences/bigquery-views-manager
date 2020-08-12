from unittest.mock import patch

import pytest

import bigquery_views_manager.config_tables as config_tables_module
from bigquery_views_manager.config_tables import update_or_create_table_from_csv

PROJECT_1 = "project1"
DATASET_1 = "dataset1"

TABLE_1 = "table1"

SOURCE_FILE_1 = "file1.csv"
SOURCE_SCHEMA_1 = "schema.json"


@pytest.fixture(name="bigquery", autouse=True)
def _bigquery():
    with patch.object(config_tables_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="LoadJobConfig")
def _query_job_config():
    with patch.object(config_tables_module, "LoadJobConfig") as mock:
        yield mock


@pytest.fixture(name="open_mock", autouse=True)
def _open():
    with patch.object(config_tables_module, "open") as mock:
        yield mock


@pytest.fixture(name="mock_exists", autouse=True)
def _path():
    with patch.object(config_tables_module.Path, "exists") as mock:
        mock.return_value = True
        yield mock


@pytest.fixture(name="get_table_schema_mock", autouse=True)
def _get_table_schema():
    with patch.object(config_tables_module, "get_table_schema") as mock:
        yield mock


# pylint: disable=invalid-name
class TestUpdateOrCreateTableFromCsv:
    def test_should_call_load_table_from_file(
            self, bq_client, LoadJobConfig, open_mock, get_table_schema_mock
    ):
        update_or_create_table_from_csv(
            bq_client,
            TABLE_1,
            SOURCE_FILE_1,
            dataset=DATASET_1,
            source_schema_file=SOURCE_SCHEMA_1,
        )

        open_mock.assert_called_with(SOURCE_FILE_1, "rb")
        source_fp = open_mock.return_value.__enter__.return_value

        get_table_schema_mock.assert_called_with(SOURCE_SCHEMA_1)

        table_ref = bq_client.dataset(DATASET_1).table(TABLE_1)
        bq_client.load_table_from_file.assert_called_with(
            source_fp, destination=table_ref, job_config=LoadJobConfig.return_value
        )

    def test_should_set_write_disposition_on_job_config(
            self, bq_client, bigquery, LoadJobConfig
    ):
        update_or_create_table_from_csv(
            bq_client,
            TABLE_1,
            SOURCE_FILE_1,
            dataset=DATASET_1,
            source_schema_file=SOURCE_SCHEMA_1,
        )
        assert (
            LoadJobConfig.return_value.write_disposition
            == bigquery.WriteDisposition.WRITE_TRUNCATE
        )

    def test_should_set_schema_on_job_config(
            self, bq_client, LoadJobConfig, get_table_schema_mock
    ):
        update_or_create_table_from_csv(
            bq_client,
            TABLE_1,
            SOURCE_FILE_1,
            dataset=DATASET_1,
            source_schema_file=SOURCE_SCHEMA_1,
        )
        assert LoadJobConfig.return_value.schema == get_table_schema_mock.return_value
