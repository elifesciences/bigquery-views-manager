from collections.abc import Set
from datetime import datetime
import textwrap
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

import bigquery_views_manager.view_dependencies as view_dependencies_module
from bigquery_views_manager.view_dependencies import (
    DatasetRef,
    LastModifiedBigQueryResultTypedDict,
    ViewDefinitionBigQueryResultTypedDict,
    get_dataset_ref_for_full_table_or_view_name,
    get_flat_view_dependencies,
    get_last_modified_timestamp_by_full_table_or_view_name_map,
    get_last_modified_timestamp_map_for_dataset_refs,
    get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref,
    get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs,
    get_view_definition_map,
    get_view_definition_query,
    get_view_dependencies,
    get_view_dependencies_from_view_definition
)
from tests.materialize_views_test import FULL_TABLE_NAME_1, FULL_TABLE_NAME_2

PROJECT_1 = 'project_1'
DATASET_1 = 'dataset_1'

DATASET_REF_1 = DatasetRef(project=PROJECT_1, dataset=DATASET_1)
DATASET_REF_2 = DatasetRef(project=PROJECT_1, dataset='dataset_2')
DATASET_REF_3 = DatasetRef(project=PROJECT_1, dataset='dataset_3')

VIEW_NAME_1 = 'view_name_1'
VIEW_NAME_2 = 'view_name_2'
VIEW_DEFINITION_1 = 'SELECT * FROM view_name_0'

FULL_VIEW_NAME_1 = f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}'
FULL_VIEW_NAME_2 = f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_2}'

TIMESTAMP_1 = datetime.fromisoformat('2001-01-01T00:00:00+00:00')

EMPTY_DICT: dict = {}
EMPTY_SET: Set = set()


@pytest.fixture(name='get_view_definition_map_mock')
def _get_view_definition_map_mock() -> Iterator[MagicMock]:
    with patch.object(view_dependencies_module, 'get_view_definition_map') as mock:
        yield mock


@pytest.fixture(name='get_last_modified_timestamp_map_for_dataset_refs_mock')
def _get_last_modified_timestamp_map_for_dataset_refs_mock() -> Iterator[MagicMock]:
    with patch.object(
        view_dependencies_module,
        'get_last_modified_timestamp_map_for_dataset_refs'
    ) as mock:
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
        assert get_view_definition_map(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        ) == EMPTY_DICT

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
        bigquery_result_row: ViewDefinitionBigQueryResultTypedDict = {
            'table_name': VIEW_NAME_1,
            'view_definition': VIEW_DEFINITION_1
        }
        iter_dict_from_bq_query_mock.return_value = iter([bigquery_result_row])
        expected_result: dict = {
            FULL_VIEW_NAME_1: VIEW_DEFINITION_1
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
        assert result == EMPTY_DICT

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
            FULL_VIEW_NAME_1: VIEW_DEFINITION_1
        }
        result = get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        expected_result: dict = {
            FULL_VIEW_NAME_1: get_view_dependencies_from_view_definition(
                VIEW_DEFINITION_1
            )
        }
        assert result == expected_result


class TestGetFlatViewDependencies:
    def test_should_return_empty_set_for_empty_view_dependencies(self):
        assert get_flat_view_dependencies(
            {}
        ) == EMPTY_SET

    def test_should_return_full_target_view_name_for_views_without_dependencies(self):
        assert get_flat_view_dependencies(
            {
                FULL_VIEW_NAME_1: set()
            }
        ) == {FULL_VIEW_NAME_1}

    def test_should_find_dependencies_across_multiple_views(self):
        assert get_flat_view_dependencies(
            {
                FULL_VIEW_NAME_1: {FULL_TABLE_NAME_1},
                FULL_VIEW_NAME_2: {FULL_TABLE_NAME_2}
            }
        ) == {
            FULL_VIEW_NAME_1,
            FULL_VIEW_NAME_2,
            FULL_TABLE_NAME_1,
            FULL_TABLE_NAME_2
        }

    def test_should_include_common_dependencies_only_once(self):
        full_common_table_name = f'{PROJECT_1}.{DATASET_1}.common_table_1'
        assert get_flat_view_dependencies(
            {
                FULL_VIEW_NAME_1: {FULL_TABLE_NAME_1, full_common_table_name},
                FULL_VIEW_NAME_2: {FULL_TABLE_NAME_2, full_common_table_name}
            }
        ) == {
            FULL_VIEW_NAME_1,
            FULL_VIEW_NAME_2,
            FULL_TABLE_NAME_1,
            FULL_TABLE_NAME_2,
            full_common_table_name
        }


class TestGetTableOrViewLastModifiedTimestampQueryForSingleDatasetRef:
    def test_should_return_query_with_project_and_dataset_replaced(self):
        assert get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref(DatasetRef(
            project='project_1',
            dataset='dataset_1'
        )) == textwrap.dedent(
            '''
            SELECT
                CONCAT(project_id, '.', dataset_id, '.', table_id) AS full_table_or_view_name,
                TIMESTAMP_MILLIS(last_modified_time) AS last_modified_timestamp
            FROM `project_1.dataset_1.__TABLES__`
            '''
        )


class TestGetTableOrViewLastModifiedTimestampQueryForMultipleDatasetRefs:
    def test_should_fail_for_empty_set(self):
        with pytest.raises(AssertionError):
            get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs(set())

    def test_should_return_query_for_single_dataset_ref(self):
        assert get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs({
            DATASET_REF_1
        }) == get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref(
            DATASET_REF_1
        )

    def test_should_return_unioned_query_for_multiple_dataset_refs(self):
        dataset_refs = [DATASET_REF_1, DATASET_REF_2, DATASET_REF_3]
        expected_query_1 = get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref(
            DATASET_REF_1
        )
        expected_query_2 = get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref(
            DATASET_REF_2
        )
        expected_query_3 = get_table_or_view_last_modified_timestamp_query_for_single_dataset_ref(
            DATASET_REF_3
        )
        expected_unioned_query = (
            expected_query_1
            + '\n\nUNION ALL\n\n'
            + expected_query_2
            + '\n\nUNION ALL\n\n'
            + expected_query_3
        )
        assert get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs(
            dataset_refs
        ) == expected_unioned_query


class TestGetLastModifiedTimestampMapForDatasetRefs:
    def test_should_return_empty_dict_if_bq_results_are_empty(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        iter_dict_from_bq_query_mock.return_value = iter([])
        assert get_last_modified_timestamp_map_for_dataset_refs(
            client=bq_client,
            dataset_refs=[DATASET_REF_1]
        ) == EMPTY_DICT

    def test_should_call_iter_dict_from_bq_query_mock(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        get_last_modified_timestamp_map_for_dataset_refs(
            client=bq_client,
            dataset_refs=[DATASET_REF_1]
        )
        iter_dict_from_bq_query_mock.assert_called_with(
            client=bq_client,
            query=get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs(
                dataset_refs=[DATASET_REF_1]
            )
        )

    def test_should_return_last_modified_timestamp_by_full_table_or_view_name_map(
        self,
        bq_client: MagicMock,
        iter_dict_from_bq_query_mock: MagicMock
    ):
        bigquery_result_row: LastModifiedBigQueryResultTypedDict = {
            'full_table_or_view_name': f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}',
            'last_modified_timestamp': TIMESTAMP_1
        }
        iter_dict_from_bq_query_mock.return_value = iter([bigquery_result_row])
        expected_result: dict = {
            f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}': TIMESTAMP_1
        }
        assert get_last_modified_timestamp_map_for_dataset_refs(
            client=bq_client,
            dataset_refs=[DATASET_REF_1]
        ) == expected_result


class TestGetDatasetRefForFullTableOrViewName:
    def test_should_parse_full_table_or_view_name(self):
        assert get_dataset_ref_for_full_table_or_view_name(
            'project_1.dataset_1.view_1'
        ) == DatasetRef(project='project_1', dataset='dataset_1')

    def test_should_fail_if_passed_in_string_does_not_have_three_parts(self):
        with pytest.raises(ValueError):
            get_dataset_ref_for_full_table_or_view_name(
                'dataset_1.view_1'
            )


class TestGetLastModifiedTimestampByFullViewOrTableMap:
    def test_should_return_empty_dict_without_dependencies(self, bq_client: MagicMock):
        assert get_last_modified_timestamp_by_full_table_or_view_name_map(
            client=bq_client,
            table_or_view_names=set()
        ) == EMPTY_DICT

    def test_should_return_dict_from_bigquery_results(
        self,
        bq_client: MagicMock,
        get_last_modified_timestamp_map_for_dataset_refs_mock: MagicMock
    ):
        get_last_modified_timestamp_map_for_dataset_refs_mock.return_value = {
            f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}': TIMESTAMP_1
        }
        assert get_last_modified_timestamp_by_full_table_or_view_name_map(
            client=bq_client,
            table_or_view_names={
                f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}'
            }
        ) == get_last_modified_timestamp_map_for_dataset_refs_mock.return_value
        get_last_modified_timestamp_map_for_dataset_refs_mock.assert_called_once_with(
            client=bq_client,
            dataset_refs={DatasetRef(project=PROJECT_1, dataset=DATASET_1)}
        )

    def test_should_return_views_from_selected_table_or_view_names(
        self,
        bq_client: MagicMock,
        get_last_modified_timestamp_map_for_dataset_refs_mock: MagicMock
    ):
        get_last_modified_timestamp_map_for_dataset_refs_mock.return_value = {
            f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}': TIMESTAMP_1,
            f'{PROJECT_1}.{DATASET_1}.other_view': TIMESTAMP_1
        }
        assert get_last_modified_timestamp_by_full_table_or_view_name_map(
            client=bq_client,
            table_or_view_names={
                f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}'
            }
        ) == {
            f'{PROJECT_1}.{DATASET_1}.{VIEW_NAME_1}': TIMESTAMP_1
        }
