from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

import bigquery_views_manager.view_dependencies as view_dependencies_module
from bigquery_views_manager.view_dependencies import (
    get_view_definition_map,
    get_view_definition_query,
    get_view_dependencies
)

PROJECT_1 = 'project_1'
DATASET_1 = 'dataset_1'


@pytest.fixture(name='get_view_definition_map_mock')
def _get_view_definition_map_mock() -> Iterator[MagicMock]:
    with patch.object(view_dependencies_module, 'get_view_definition_map') as mock:
        yield mock


class TestGetViewDefinitionQuery:
    def test_should_return_query(self):
        assert get_view_definition_query(
            project=PROJECT_1,
            dataset=DATASET_1
        ) == (
            'SELECT table_name, view_definition\n'
            f'FROM `{PROJECT_1}.{DATASET_1}.INFORMATION_SCHEMA.VIEWS`'
        )


class TestGetViewDefinitionMap:
    def test_should_return_empty_dict_if_bq_results_are_empty(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        iter_dict_from_bq_query_mock.return_value = iter([])
        expected_result: dict = {}
        assert get_view_definition_map(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        ) == expected_result

    def test_should_call_iter_dict_from_bq_query_mock(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        get_view_definition_map(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        iter_dict_from_bq_query_mock.assert_called_with(
            client=bq_client,
            query=get_view_definition_query(
                project=PROJECT_1,
                dataset=DATASET_1
            )
        )


class TestGetViewDependencies:
    def test_should_return_empty_dict_when_there_are_no_views(
        self,
        bq_client: MagicMock
    ):
        result = get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        expected_result: dict = {}
        assert result == expected_result

    def test_should_retrieve_view_definitions_for_dataset(
        self,
        bq_client: MagicMock,
        get_view_definition_map_mock: MagicMock
    ):
        get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        get_view_definition_map_mock.assert_called_with(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
