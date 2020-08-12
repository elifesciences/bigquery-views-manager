from collections import OrderedDict

from unittest.mock import patch

import pytest

from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY
import bigquery_views_manager.diff_views as diff_views_module
from bigquery_views_manager.diff_views import get_diff_result

PROJECT_1 = "project1"
DATASET_1 = "dataset1"

VIEW_1 = "view1"
VIEW_2 = "view2"

VIEW_TO_DATASET_MAPPING = {VIEW_1: DATASET_1}

VIEW_QUERY_1 = "SELECT * FROM `project1.dataset1.table1`"
VIEW_QUERY_2 = "SELECT * FROM `project1.dataset1.table2`"

BASE_DIR = "views"


def get_input_ordered_dict_view_mapping():
    view_mapping = OrderedDict()
    view_mapping[VIEW_1] = {
        DATASET_NAME_KEY: DATASET_1,
        VIEW_OR_TABLE_NAME_KEY: VIEW_1
    }
    return view_mapping


@pytest.fixture(name="bigquery", autouse=True)
def _bigquery():
    with patch.object(diff_views_module, "bigquery") as mock:
        yield mock


@pytest.fixture(name="get_local_view_query", autouse=True)
def _get_local_view_query():
    with patch.object(diff_views_module, "get_local_view_query") as mock:
        yield mock


@pytest.fixture(name="get_bq_view_query", autouse=True)
def _get_bq_view_query():
    with patch.object(diff_views_module, "get_bq_view_query") as mock:
        yield mock


@pytest.fixture(name="get_bq_view_names", autouse=True)
def _get_bq_view_names():
    with patch.object(diff_views_module, "get_bq_view_names") as mock:
        yield mock


class TestGetDiffResult:
    def test_should_return_result_for_single_unchanged_view(
            self, bq_client, get_local_view_query, get_bq_view_query,
            get_bq_view_names):
        get_local_view_query.return_value = VIEW_QUERY_1
        get_bq_view_query.return_value = VIEW_QUERY_1
        get_bq_view_names.return_value = [VIEW_1]
        diff_result = get_diff_result(
            bq_client,
            BASE_DIR,
            view_names_dict=get_input_ordered_dict_view_mapping(),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        assert diff_result.unchanged_view_names == {
            ".".join([DATASET_1, VIEW_1])
        }
        assert diff_result.local_only_view_names == set()
        assert diff_result.remote_only_view_names == set()
        assert diff_result.changed_view_names == set()
        assert diff_result.changed_views == []

    def test_should_return_result_for_single_changed_view(
            self, bq_client, get_local_view_query, get_bq_view_query,
            get_bq_view_names):
        get_local_view_query.return_value = VIEW_QUERY_1
        get_bq_view_query.return_value = VIEW_QUERY_2
        get_bq_view_names.return_value = [VIEW_1]
        diff_result = get_diff_result(
            bq_client,
            BASE_DIR,
            view_names_dict=get_input_ordered_dict_view_mapping(),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        assert diff_result.unchanged_view_names == set()
        assert diff_result.local_only_view_names == set()
        assert diff_result.remote_only_view_names == set()
        assert diff_result.changed_view_names == {VIEW_1}

        assert len(diff_result.changed_views) == 1
        changed_view = diff_result.changed_views[0]
        assert changed_view.view_name == VIEW_1
        assert changed_view.local_view_query == VIEW_QUERY_1
        assert changed_view.remote_view_query == VIEW_QUERY_2

    def test_should_return_for_single_local_only_view(self, bq_client,
                                                      get_local_view_query,
                                                      get_bq_view_query,
                                                      get_bq_view_names):
        get_local_view_query.return_value = VIEW_QUERY_1
        get_bq_view_query.return_value = VIEW_QUERY_2
        get_bq_view_names.return_value = []
        diff_result = get_diff_result(
            bq_client,
            BASE_DIR,
            view_names_dict=get_input_ordered_dict_view_mapping(),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        assert diff_result.unchanged_view_names == set()
        assert diff_result.local_only_view_names == {
            ".".join([DATASET_1, VIEW_1])
        }
        assert diff_result.remote_only_view_names == set()
        assert diff_result.changed_view_names == set()

    def test_should_return_for_single_remote_only_view(self, bq_client,
                                                       get_local_view_query,
                                                       get_bq_view_query,
                                                       get_bq_view_names):
        get_local_view_query.return_value = VIEW_QUERY_1
        get_bq_view_query.return_value = VIEW_QUERY_2
        get_bq_view_names.return_value = [VIEW_2]
        diff_result = get_diff_result(
            bq_client,
            BASE_DIR,
            view_names_dict=get_input_ordered_dict_view_mapping(),
            project=PROJECT_1,
            default_dataset=DATASET_1,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        )
        assert diff_result.unchanged_view_names == set()
        assert diff_result.local_only_view_names == {
            ".".join([DATASET_1, VIEW_1])
        }
        assert diff_result.remote_only_view_names == {
            ".".join([DATASET_1, VIEW_2])
        }
        assert diff_result.changed_view_names == set()
