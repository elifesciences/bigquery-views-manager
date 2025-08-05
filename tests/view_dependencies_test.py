from bigquery_views_manager.view_dependencies import get_view_dependencies

PROJECT_1 = 'project1'


class TestGetViewDependencies:
    def test_should_return_empty_dict_when_the_view_list_is_empty(self, bq_client):
        result = get_view_dependencies(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=[]
        )
        expected_result = {}
        assert result == expected_result
