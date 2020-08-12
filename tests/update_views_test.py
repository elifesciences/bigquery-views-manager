from collections import OrderedDict

from unittest.mock import patch, MagicMock

import pytest

import bigquery_views_manager.update_views as update_views_module
from bigquery_views_manager.update_views import (
    get_create_or_replace_view_query,
    update_or_create_view,
    update_or_create_views,
)
from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

PROJECT_1 = "project1"
DATASET_1 = "dataset1"

VIEW_1 = "view1"
M_VIEW_1 = "materialized_view1"
OTHER_VIEW = "other"
VIEW_TO_DATASET_MAPPING = {VIEW_1: DATASET_1}

VIEW_QUERY_1 = "SELECT * FROM `project1.dataset1.table1`"

BASE_DIR_1 = "/views1"


def get_input_ordered_dict_view_mapping(view_name: str, db_view_name: str):
    view_mapping = OrderedDict()
    view_mapping[view_name] = {
        DATASET_NAME_KEY: DATASET_1,
        VIEW_OR_TABLE_NAME_KEY: db_view_name,
    }
    return view_mapping


@pytest.fixture(name="bigquery", autouse=True)
def _bigquery():
    with patch.object(update_views_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="get_local_view_query", autouse=True)
def _get_local_view_query():
    with patch.object(update_views_module, "get_local_view_query") as mock:
        yield mock


@pytest.fixture(name="materialize_view", autouse=True)
def _materialize_view():
    with patch.object(update_views_module, "materialize_view") as mock:
        yield mock


class TestGetCreateOrReplaceViewQuery:
    def test_should_generate_query(self):
        view = MagicMock()
        view.dataset_id = DATASET_1
        view.table_id = VIEW_1
        view.view_query = VIEW_QUERY_1
        assert get_create_or_replace_view_query(view) == (
            f"CREATE OR REPLACE VIEW {DATASET_1}.{VIEW_1} AS {VIEW_QUERY_1}")


class TestUpdateOrCreateView:
    def test_should_call_query_with_create_or_replace_view_query(
            self, bq_client, bigquery):
        bigquery.Table.return_value.dataset_id = DATASET_1
        bigquery.Table.return_value.table_id = VIEW_1
        update_or_create_view(bq_client,
                              VIEW_1,
                              VIEW_QUERY_1,
                              dataset=DATASET_1)
        bq_client.query.assert_called_with(
            f"CREATE OR REPLACE VIEW {DATASET_1}.{VIEW_1} AS {VIEW_QUERY_1}")

    def test_should_call_result_on_query_job(self, bq_client):
        update_or_create_view(bq_client,
                              VIEW_1,
                              VIEW_QUERY_1,
                              dataset=DATASET_1)
        bq_client.query.return_value.result.assert_called_with()


class TestUpdateOrCreateViews:
    def test_should_materialize_view_if_in_materialized_view_names(
            self, bq_client, materialize_view):
        update_or_create_views(
            bq_client,
            BASE_DIR_1,
            view_names_dict=get_input_ordered_dict_view_mapping(
                VIEW_1, VIEW_1),
            materialized_view_names=get_input_ordered_dict_view_mapping(
                VIEW_1, M_VIEW_1),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        materialize_view.assert_called_with(
            bq_client,
            source_view_name=VIEW_1,
            destination_table_name=get_input_ordered_dict_view_mapping(
                VIEW_1, M_VIEW_1).get(VIEW_1).get(VIEW_OR_TABLE_NAME_KEY),
            project=PROJECT_1,
            source_dataset=get_input_ordered_dict_view_mapping(
                VIEW_1, VIEW_1).get(VIEW_1).get(DATASET_NAME_KEY),
            destination_dataset=get_input_ordered_dict_view_mapping(
                VIEW_1, M_VIEW_1).get(VIEW_1).get(DATASET_NAME_KEY),
        )

    def test_should_not_materialize_view_if_not_in_materialized_view_names(
            self, bq_client, materialize_view):
        update_or_create_views(
            bq_client,
            BASE_DIR_1,
            view_names_dict=get_input_ordered_dict_view_mapping(
                VIEW_1, VIEW_1),
            materialized_view_names=get_input_ordered_dict_view_mapping(
                OTHER_VIEW, OTHER_VIEW),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        materialize_view.assert_not_called()
