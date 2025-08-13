from datetime import datetime
from typing import Iterator, OrderedDict
from unittest.mock import ANY, MagicMock, patch

import pytest

from bigquery_views_manager.view_list import ViewConfig, ViewListConfig
import bigquery_views_manager.materialize_views as materialize_views_module
from bigquery_views_manager.materialize_views import (
    MaterializeViewListResult,
    MaterializeViewResult,
    MaterializeViewState,
    get_select_all_from_query,
    materialize_view,
    materialize_views,
    materialize_views_if_necessary,
    materialize_views_if_necessary_with_state
)
from bigquery_views_manager.materialize_views_typing import DatasetViewDataTypedDict

PROJECT_1 = 'project1'
SOURCE_DATASET_1 = 'dataset1'
DESTINATION_DATASET_1 = 'dataset2'

VIEW_1 = 'view1'
VIEW_2 = 'view2'

TABLE_1 = 'table1'
TABLE_2 = 'table2'

FULL_VIEW_NAME_1 = f'{PROJECT_1}.{SOURCE_DATASET_1}.{VIEW_1}'
FULL_VIEW_NAME_2 = f'{PROJECT_1}.{SOURCE_DATASET_1}.{VIEW_2}'
FULL_TABLE_NAME_1 = f'{PROJECT_1}.{SOURCE_DATASET_1}.{TABLE_1}'
FULL_TABLE_NAME_2 = f'{PROJECT_1}.{SOURCE_DATASET_1}.{TABLE_2}'

VIEW_QUERY_1 = 'SELECT * FROM `project1.dataset1.table1`'

TIMESTAMP_1 = datetime.fromisoformat('2001-01-01T00:00:00+00:00')
TIMESTAMP_2 = datetime.fromisoformat('2001-01-02T00:00:00+00:00')
TIMESTAMP_3 = datetime.fromisoformat('2001-01-03T00:00:00+00:00')


@pytest.fixture(name='bigquery', autouse=True)
def _bigquery():
    with patch.object(materialize_views_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='QueryJobConfig')
def _query_job_config():
    with patch.object(materialize_views_module, 'QueryJobConfig') as mock:
        yield mock


@pytest.fixture(name='get_current_timestamp_mock')
def _get_current_timestamp_mock() -> Iterator[MagicMock]:
    with patch.object(materialize_views_module, 'get_current_timestamp') as mock:
        yield mock


@pytest.fixture(name='get_view_dependencies_mock')
def _get_view_dependencies_mock() -> Iterator[MagicMock]:
    with patch.object(materialize_views_module, 'get_view_dependencies') as mock:
        mock.return_value = {}
        yield mock


@pytest.fixture(name='get_last_modified_timestamp_by_full_table_or_view_name_map_mock')
def _get_last_modified_timestamp_by_full_table_or_view_name_map_mock() -> Iterator[MagicMock]:
    with patch.object(
        materialize_views_module,
        'get_last_modified_timestamp_by_full_table_or_view_name_map'
    ) as mock:
        mock.return_value = {}
        yield mock


@pytest.fixture(name='materialize_views_if_necessary_with_state_mock')
def _materialize_views_if_necessary_with_state_mock() -> Iterator[MagicMock]:
    with patch.object(
        materialize_views_module,
        'materialize_views_if_necessary_with_state'
    ) as mock:
        yield mock


class TestGetSelectAllFromQuery:
    def test_should_substitute_values(self):
        assert (get_select_all_from_query(
            VIEW_1, project=PROJECT_1, dataset=SOURCE_DATASET_1)) == (
                f'SELECT * FROM `{PROJECT_1}.{SOURCE_DATASET_1}.{VIEW_1}`')


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
            get_select_all_from_query(VIEW_1, project=PROJECT_1, dataset=SOURCE_DATASET_1),
            job_config=QueryJobConfig.return_value,
        )

    def test_should_set_write_disposition_on_job_config(
        self,
        bq_client,
        bigquery,
        QueryJobConfig
    ):
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
            materialized_view_dict=OrderedDict[str, DatasetViewDataTypedDict](),
            source_view_dict=OrderedDict[str, DatasetViewDataTypedDict](),
            project=PROJECT_1
        )
        assert return_value == MaterializeViewListResult(result_list=[])
        assert not return_value

    def test_should_return_result(self, bq_client):
        destination_dataset_view_dict: DatasetViewDataTypedDict = {
            'dataset_name': DESTINATION_DATASET_1,
            'table_name': TABLE_1
        }
        source_dataset_view_dict: DatasetViewDataTypedDict = {
            'dataset_name': SOURCE_DATASET_1,
            'table_name': VIEW_1
        }
        materialized_view_dict = OrderedDict[str, DatasetViewDataTypedDict]({
            'view_template_file_name_1': destination_dataset_view_dict
        })
        source_view_dict = OrderedDict[str, DatasetViewDataTypedDict]({
            'view_template_file_name_1': source_dataset_view_dict
        })
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


class TestMaterializeViewState:
    class TestUpdateTimestamp:
        def test_should_update_last_modified_timestamp(self):
            state = MaterializeViewState(
                view_dependencies={VIEW_1: {FULL_TABLE_NAME_1}},
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1,
                    FULL_TABLE_NAME_1: TIMESTAMP_2
                }
            )
            assert state.get_timestamp(FULL_TABLE_NAME_1) == TIMESTAMP_2
            state.update_timestamp(FULL_TABLE_NAME_1, TIMESTAMP_3)
            assert state.get_timestamp(FULL_TABLE_NAME_1) == TIMESTAMP_3

    class TestGetLatestTimestampOfDependencies:
        def test_should_fail_if_view_is_not_in_dependencies_map(self):
            state = MaterializeViewState(
                view_dependencies={VIEW_1: set()},
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1
                }
            )
            with pytest.raises(KeyError):
                state.get_latest_timestamp_of_dependencies(VIEW_2)

        def test_should_return_none_if_view_has_no_dependencies(self):
            state = MaterializeViewState(
                view_dependencies={VIEW_1: set()},
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1
                }
            )
            assert state.get_latest_timestamp_of_dependencies(VIEW_1) is None

        def test_should_return_timestamp_of_single_dependency(self):
            state = MaterializeViewState(
                view_dependencies={VIEW_1: {FULL_TABLE_NAME_1}},
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1,
                    FULL_TABLE_NAME_1: TIMESTAMP_2
                }
            )
            assert state.get_latest_timestamp_of_dependencies(VIEW_1) == TIMESTAMP_2

        def test_should_return_latest_timestamp_of_multiple_dependencies(self):
            state = MaterializeViewState(
                view_dependencies={VIEW_1: {FULL_TABLE_NAME_1, FULL_TABLE_NAME_2}},
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1,
                    FULL_TABLE_NAME_1: TIMESTAMP_2,
                    FULL_TABLE_NAME_2: TIMESTAMP_3
                }
            )
            assert state.get_latest_timestamp_of_dependencies(VIEW_1) == TIMESTAMP_3

        def test_should_return_timestamp_of_indirect_dependencies(self):
            state = MaterializeViewState(
                view_dependencies={
                    VIEW_1: {FULL_TABLE_NAME_1, FULL_VIEW_NAME_2},
                    VIEW_2: {FULL_TABLE_NAME_2}
                },
                last_modified_timestamp_map={
                    FULL_VIEW_NAME_1: TIMESTAMP_1,
                    FULL_VIEW_NAME_2: TIMESTAMP_1,
                    FULL_TABLE_NAME_1: TIMESTAMP_2,
                    FULL_TABLE_NAME_2: TIMESTAMP_3
                }
            )
            assert state.get_latest_timestamp_of_dependencies(VIEW_1) == TIMESTAMP_3


class TestMaterializeViewsIfNecessaryWithState:
    def test_should_return_empty_list_when_there_is_no_views(self, bq_client):
        return_value = materialize_views_if_necessary_with_state(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=ViewListConfig([]),
            state=MaterializeViewState(
                view_dependencies={},
                last_modified_timestamp_map={}
            )
        )
        assert return_value == MaterializeViewListResult(result_list=[])
        assert not return_value

    def test_should_update_timestamp_after_materializing(
        self,
        bq_client: MagicMock,
        get_current_timestamp_mock: MagicMock
    ):
        state = MaterializeViewState(
            view_dependencies={VIEW_1: {FULL_TABLE_NAME_1}},
            last_modified_timestamp_map={
                FULL_VIEW_NAME_1: TIMESTAMP_1,
                FULL_TABLE_NAME_1: TIMESTAMP_2
            }
        )
        assert state.get_timestamp(FULL_TABLE_NAME_1) == TIMESTAMP_2
        get_current_timestamp_mock.return_value = TIMESTAMP_3
        materialize_views_if_necessary_with_state(
            client=bq_client,
            project=PROJECT_1,
            dataset=SOURCE_DATASET_1,
            view_list_config=ViewListConfig([
                ViewConfig(
                    view_name=VIEW_1,
                    materialize_as=f'{SOURCE_DATASET_1}.{TABLE_1}'
                )
            ]),
            state=state
        )
        assert state.get_timestamp(FULL_TABLE_NAME_1) == TIMESTAMP_3


class TestMaterializeViewsIfNecessary:
    def test_should_return_empty_list_when_there_is_no_view_to_materialize(self, bq_client):
        return_value = materialize_views_if_necessary(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=ViewListConfig([
                ViewConfig(
                    view_name=VIEW_1,
                    materialize=False
                )
            ])
        )
        assert return_value == MaterializeViewListResult(result_list=[])
        assert not return_value

    def test_should_return_result(
        self,
        bq_client: MagicMock,
        get_view_dependencies_mock: MagicMock,
        get_last_modified_timestamp_by_full_table_or_view_name_map_mock: MagicMock
    ):
        get_view_dependencies_mock.return_value = {VIEW_1: set()}
        get_last_modified_timestamp_by_full_table_or_view_name_map_mock.return_value = {
            FULL_VIEW_NAME_1: TIMESTAMP_1
        }
        return_value = materialize_views_if_necessary(
            client=bq_client,
            project=PROJECT_1,
            dataset=SOURCE_DATASET_1,
            view_list_config=ViewListConfig([
                ViewConfig(
                    view_name=VIEW_1,
                    materialize_as=f'{DESTINATION_DATASET_1}.{TABLE_1}'
                )
            ])
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

    def test_should_only_materialize_selected_views(
        self,
        bq_client: MagicMock,
        get_view_dependencies_mock: MagicMock,
        get_last_modified_timestamp_by_full_table_or_view_name_map_mock: MagicMock
    ):
        get_view_dependencies_mock.return_value = {
            VIEW_1: set(),
            VIEW_2: set()
        }
        get_last_modified_timestamp_by_full_table_or_view_name_map_mock.return_value = {
            FULL_VIEW_NAME_1: TIMESTAMP_1,
            FULL_VIEW_NAME_2: TIMESTAMP_2
        }
        return_value = materialize_views_if_necessary(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=ViewListConfig([
                ViewConfig(
                    view_name=VIEW_1,
                    materialize=True
                ),
                ViewConfig(
                    view_name=VIEW_2,
                    materialize=True
                )
            ]),
            selected_view_names=[VIEW_1]
        )
        assert len(return_value.result_list) == 1
        assert return_value.result_list[0].source_view_name == VIEW_1

    def test_should_call_materialize_views_if_necessary_with_state(
        self,
        bq_client: MagicMock,
        materialize_views_if_necessary_with_state_mock: MagicMock,
        get_view_dependencies_mock: MagicMock,
        get_last_modified_timestamp_by_full_table_or_view_name_map_mock: MagicMock
    ):
        view_list_config = ViewListConfig([
            ViewConfig(
                view_name=VIEW_1,
                materialize=True
            )
        ])
        return_value = materialize_views_if_necessary(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=view_list_config,
            selected_view_names=[VIEW_1]
        )
        assert return_value == materialize_views_if_necessary_with_state_mock.return_value
        materialize_views_if_necessary_with_state_mock.assert_called_with(
            client=bq_client,
            project=PROJECT_1,
            dataset='dataset_1',
            view_list_config=view_list_config,
            state=MaterializeViewState(
                view_dependencies=get_view_dependencies_mock.return_value,
                last_modified_timestamp_map=(
                    get_last_modified_timestamp_by_full_table_or_view_name_map_mock.return_value
                )
            ),
            selected_view_names=[VIEW_1]
        )
