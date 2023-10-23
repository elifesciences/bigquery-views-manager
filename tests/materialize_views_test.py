from unittest.mock import ANY, patch

import pytest

import bigquery_views_manager.materialize_views as materialize_views_module
from bigquery_views_manager.materialize_views import (
    MaterializeViewListResult,
    MaterializeViewResult,
    get_select_all_from_query,
    materialize_view,
    materialize_views
)
from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

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

    def test_should_return_results(self, bq_client):
        return_value = materialize_view(
            bq_client,
            source_view_name=VIEW_1,
            destination_table_name=TABLE_1,
            project=PROJECT_1,
            source_dataset=SOURCE_DATASET_1,
            destination_dataset=DESTINATION_DATASET_1,
        )
        query_job = bq_client.query.return_value
        bq_result = query_job.result.return_value
        assert return_value
        assert return_value.duration is not None
        assert return_value.total_rows == bq_result.total_rows
        assert return_value.total_bytes_processed == query_job.total_bytes_processed
        assert return_value.cache_hit == query_job.cache_hit
        assert return_value.slot_millis == query_job.slot_millis
        assert return_value.total_bytes_billed == return_value.total_bytes_billed
        assert return_value.source_dataset == SOURCE_DATASET_1
        assert return_value.source_view_name == VIEW_1
        assert return_value.destination_dataset == DESTINATION_DATASET_1
        assert return_value.destination_table_name == TABLE_1


class TestMaterializeViews:
    def test_should_return_empty_list_when_there_is_no_view_to_materialize(self, bq_client):
        return_value = materialize_views(
            client=bq_client,
            materialized_view_dict={},
            source_view_dict={},
            project=PROJECT_1
        )
        assert return_value == MaterializeViewListResult(result_list=[])
        assert not return_value

    def test_should_return_result(self, bq_client):
        destination_dataset_view_dict = {
            DATASET_NAME_KEY: DESTINATION_DATASET_1,
            VIEW_OR_TABLE_NAME_KEY: TABLE_1
        }
        source_dataset_view_dict = {
            DATASET_NAME_KEY: SOURCE_DATASET_1,
            VIEW_OR_TABLE_NAME_KEY: VIEW_1
        }
        materialized_view_dict = {'view_template_file_name_1': destination_dataset_view_dict}
        source_view_dict = {'view_template_file_name_1': source_dataset_view_dict}
        return_value = materialize_views(
            client=bq_client,
            materialized_view_dict=materialized_view_dict,
            source_view_dict=source_view_dict,
            project=PROJECT_1
        )
        assert return_value == MaterializeViewListResult(
            result_list=[MaterializeViewResult(
                source_dataset=SOURCE_DATASET_1,
                source_view_name=VIEW_1,
                destination_dataset=DESTINATION_DATASET_1,
                destination_table_name=TABLE_1,
                total_bytes_processed=ANY,
                total_rows=ANY,
                duration=ANY,
                cache_hit=ANY,
                slot_millis=ANY,
                total_bytes_billed=ANY
            )]
        )
