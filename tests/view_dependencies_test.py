from collections.abc import Set
import textwrap
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

import bigquery_views_manager.view_dependencies as view_dependencies_module
from bigquery_views_manager.view_dependencies import (
    get_view_definition_map,
    get_view_definition_query,
    get_view_dependencies,
    get_view_dependencies_from_view_definition
)

PROJECT_1 = 'project_1'
DATASET_1 = 'dataset_1'
VIEW_NAME_1 = 'view_name_1'
VIEW_DEFINITION_1 = 'SELECT * FROM view_name_0'


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

    def test_should_return_view_definition_map(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        iter_dict_from_bq_query_mock.return_value = iter([{
            'table_name': VIEW_NAME_1,
            'view_definition': VIEW_DEFINITION_1
        }])
        expected_result: dict = {
            VIEW_NAME_1: VIEW_DEFINITION_1
        }
        assert get_view_definition_map(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        ) == expected_result


class TestGetViewDependenciesFromViewDefinition:
    def test_should_return_empty_sequence_if_view_has_no_from(self):
        expected_result: Set = set()
        assert get_view_dependencies_from_view_definition('SELECT 1') == expected_result

    def test_should_return_set_of_dependencies_when_there_is_a_from(self):
        result = get_view_dependencies_from_view_definition(
            'SELECT * FROM project_1.dataset_1.table_1'
        )
        assert result == {'project_1.dataset_1.table_1'}

    def test_should_return_set_of_dependencies_with_line_feed_before_from(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT *
            FROM project_1.dataset_1.table_1
            '''
        ))
        assert result == {'project_1.dataset_1.table_1'}

    def test_should_return_set_of_dependencies_for_project_with_hyphens(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT *
            FROM project-1.dataset_1.table_1
            '''
        ))
        assert result == {'project-1.dataset_1.table_1'}

    def test_should_ignore_dependencies_without_a_project(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT * FROM dataset_1.table_1
            CROSS JOIN project_1.dataset_1.table_2
            '''
        ))
        assert result == {'project_1.dataset_1.table_2'}

    def test_should_not_return_same_dependency_twice(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT * FROM project_1.dataset_1.table_1
            UNION ALL
            SELECT * FROM project_1.dataset_1.table_1
            '''
        ))
        assert result == {'project_1.dataset_1.table_1'}

    def test_should_return_set_of_dependencies_when_there_is_a_from_with_backticks(self):
        result = get_view_dependencies_from_view_definition(
            'SELECT * FROM `project_1.dataset_1.table_1`'
        )
        assert result == {'project_1.dataset_1.table_1'}

    def test_should_return_set_of_dependencies_when_there_is_joined_table(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT *
            FROM project_1.dataset_1.table_1 AS t1
            JOIN project_1.dataset_1.table_2 AS t2
              ON t1.id = t2.id
            '''
        ))
        assert result == {'project_1.dataset_1.table_1', 'project_1.dataset_1.table_2'}

    def test_should_return_set_of_dependencies_when_there_are_multiple_joins(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT *
            FROM project_1.dataset_1.table_1 AS t1
            JOIN project_1.dataset_1.table_2
              ON t1.id = table_2.id
            LEFT JOIN project_1.dataset_1.table_3
              ON t1.id = table_3.id
            '''
        ))
        assert result == {
            'project_1.dataset_1.table_1',
            'project_1.dataset_1.table_2',
            'project_1.dataset_1.table_3'
        }

    def test_should_return_set_of_dependencies_when_there_are_sub_queries(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            WITH t_table_0 AS (
               SELECT *
               FROM project_1.dataset_1.table_1
               JOIN project_1.dataset_1.table_2 ON table_1.id = table_2.id
               JOIN project_1.dataset_1.table_3 ON table_2.id = table_3.id
            ),
            t_table_00 AS (
               SELECT *
               FROM t_table_0
               JOIN project_1.dataset_1.table_4 ON t_table_0.id = table_4.id
            )
            SELECT *
            FROM t_table_00
            '''
        ))
        assert result == {
            'project_1.dataset_1.table_1',
            'project_1.dataset_1.table_2',
            'project_1.dataset_1.table_3',
            'project_1.dataset_1.table_4'
        }

    def test_should_ignore_implicit_unnest_from_as_alias(self):
        result = get_view_dependencies_from_view_definition(textwrap.dedent(
            '''
            SELECT *
            FROM `project_1.dataset_1.table_1` AS t
            JOIN t.nested AS n
            '''
        ))
        assert result == {'project_1.dataset_1.table_1'}


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

    def test_should_return_dependency_dict(
        self,
        bq_client: MagicMock,
        get_view_definition_map_mock: MagicMock
    ):
        get_view_definition_map_mock.return_value = {
            VIEW_NAME_1: VIEW_DEFINITION_1
        }
        result = get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        expected_result: dict = {
            VIEW_NAME_1: get_view_dependencies_from_view_definition(
                VIEW_DEFINITION_1
            )
        }
        assert result == expected_result
