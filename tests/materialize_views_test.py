from unittest.mock import patch

import pytest

import bigquery_views_manager.materialize_views as materialize_views_module
from bigquery_views_manager.materialize_views import get_select_all_from_query, materialize_view

PROJECT_1 = "project1"
SOURCE_DATASET_1 = "dataset1"
DESTINATION_DATASET_1 = "dataset2"

VIEW_1 = "view1"
VIEW_2 = "view2"

TABLE_1 = "table1"

VIEW_QUERY_1 = "SELECT * FROM `project1.dataset1.table1`"


@pytest.fixture(name="bigquery", autouse=True)
def _bigquery():
    with patch.object(materialize_views_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="QueryJobConfig")
def _query_job_config():
    with patch.object(materialize_views_module, "QueryJobConfig") as mock:
        yield mock


class TestGetSelectAllFromQuery:
    def test_should_substitute_values(self):
        assert (get_select_all_from_query(
            VIEW_1, project=PROJECT_1, dataset=SOURCE_DATASET_1)) == (
                f"SELECT * FROM `{PROJECT_1}.{SOURCE_DATASET_1}.{VIEW_1}`")


# pylint: disable=invalid-name
class TestMaterializeView:
    def test_should_call_query(self, bq_client, QueryJobConfig):
        materialize_view(
            bq_client,
            source_view_name=VIEW_1,
            destination_table_name=TABLE_1,
            project=PROJECT_1,
            source_dataset=SOURCE_DATASET_1,
            destination_dataset=DESTINATION_DATASET_1,
        )
        bq_client.query.assert_called_with(
            get_select_all_from_query(VIEW_1,
                                      project=PROJECT_1,
                                      dataset=SOURCE_DATASET_1),
            job_config=QueryJobConfig.return_value,
        )

    def test_should_set_write_disposition_on_job_config(
            self, bq_client, bigquery, QueryJobConfig):
        materialize_view(
            bq_client,
            source_view_name=VIEW_1,
            destination_table_name=TABLE_1,
            project=PROJECT_1,
            source_dataset=SOURCE_DATASET_1,
            destination_dataset=DESTINATION_DATASET_1,
        )
        assert (QueryJobConfig.return_value.write_disposition ==
                bigquery.WriteDisposition.WRITE_TRUNCATE)

    def test_should_call_result_on_query_job(self, bq_client):
        materialize_view(
            bq_client,
            source_view_name=VIEW_1,
            destination_table_name=TABLE_1,
            project=PROJECT_1,
            source_dataset=SOURCE_DATASET_1,
            destination_dataset=DESTINATION_DATASET_1,
        )
        bq_client.query.return_value.result.assert_called()
