from unittest.mock import MagicMock

from bigquery_views_manager.view_dependencies import (
    get_view_definition_query,
    get_view_dependencies
)

PROJECT_1 = 'project_1'
DATASET_1 = 'dataset_1'


class TestGetViewDefinitionQuery:
    def test_should_return_query(self):
        assert get_view_definition_query(
            project=PROJECT_1,
            dataset=DATASET_1
        ) == (
            'SELECT table_name, view_definition\n'
            f'FROM `{PROJECT_1}.{DATASET_1}.INFORMATION_SCHEMA.VIEWS`'
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
        bq_client: MagicMock
    ):
        get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset=DATASET_1
        )
        bq_client.query.assert_called_with(
            get_view_definition_query(
                project=PROJECT_1,
                dataset=DATASET_1
            )
        )
