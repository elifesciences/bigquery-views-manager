from datetime import datetime, timezone
import logging
import time
from collections import OrderedDict
from collections.abc import Container, Set
from itertools import islice
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig

from bigquery_views_manager.utils.json import get_json
from bigquery_views_manager.view_dependencies import (
    get_flat_view_dependencies,
    get_last_modified_timestamp_by_full_table_or_view_name_map,
    get_view_dependencies
)
from bigquery_views_manager.materialize_views_typing import DatasetViewDataTypedDict
from bigquery_views_manager.view_list import ViewListConfig

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MaterializeViewResult:  # pylint: disable=too-many-instance-attributes
    source_dataset: str
    source_view_name: str
    destination_dataset: str
    destination_table_name: str
    total_bytes_processed: Optional[int]
    total_rows: Optional[int]
    duration: float
    cache_hit: bool
    slot_millis: Optional[int]
    total_bytes_billed: int


@dataclass(frozen=True)
class MaterializeViewListResult:
    result_list: Sequence[MaterializeViewResult]

    def __bool__(self):
        return bool(self.result_list)


def get_current_timestamp() -> datetime:
    return datetime.now(timezone.utc)


def get_select_all_from_query(
    view_name: str,
    project: str,
    dataset: str
) -> str:
    return f'SELECT * FROM `{project}.{dataset}.{view_name}`'


def materialize_view(  # pylint: disable=too-many-arguments, too-many-locals
    client: bigquery.Client,
    source_view_name: str,
    destination_table_name: str,
    project: str,
    source_dataset: str,
    destination_dataset: str,
) -> MaterializeViewResult:
    query = get_select_all_from_query(source_view_name, project=project, dataset=source_dataset)
    LOGGER.info(
        'materializing view: %s.%s -> %s.%s',
        source_dataset,
        source_view_name,
        destination_dataset,
        destination_table_name
    )
    LOGGER.debug('materialize_view: %s=%s', destination_table_name, [query])

    start = time.perf_counter()
    dataset_ref = client.dataset(destination_dataset)
    destination_table_ref = dataset_ref.table(destination_table_name)

    job_config = QueryJobConfig()
    job_config.destination = destination_table_ref
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    query_job = client.query(query, job_config=job_config)
    # getting the result will make sure that the query ran successfully
    result: bigquery.table.RowIterator = query_job.result()
    duration = time.perf_counter() - start
    total_bytes_processed = query_job.total_bytes_processed
    cache_hit = query_job.cache_hit
    slot_millis = query_job.slot_millis
    total_bytes_billed = query_job.total_bytes_billed
    LOGGER.info(
        'materialized view: %s.%s, total rows: %s, %s bytes processed, took: %.3fs',
        source_dataset,
        source_view_name,
        result.total_rows,
        total_bytes_processed,
        duration
    )
    if LOGGER.isEnabledFor(logging.DEBUG):
        sample_result = list(islice(result, 3))
        LOGGER.debug('sample_result: %s', sample_result)
    return MaterializeViewResult(
        source_dataset=source_dataset,
        source_view_name=source_view_name,
        destination_dataset=destination_dataset,
        destination_table_name=destination_table_name,
        total_bytes_processed=total_bytes_processed,
        total_rows=result.total_rows,
        duration=duration,
        cache_hit=cache_hit,
        slot_millis=slot_millis,
        total_bytes_billed=total_bytes_billed
    )


def materialize_views(
    client: bigquery.Client,
    materialized_view_dict: OrderedDict[str, DatasetViewDataTypedDict],
    source_view_dict: OrderedDict[str, DatasetViewDataTypedDict],
    project: str,
) -> MaterializeViewListResult:
    LOGGER.info('view_names: %s', materialized_view_dict)
    if not materialized_view_dict:
        return MaterializeViewListResult(result_list=[])
    start = time.perf_counter()
    total_bytes_processed = 0
    total_rows = 0
    result_list = []
    for view_template_file_name, dataset_view_data in materialized_view_dict.items():
        result = materialize_view(
            client,
            source_view_name=source_view_dict[view_template_file_name]['table_name'],
            destination_table_name=dataset_view_data['table_name'],
            project=project,
            source_dataset=source_view_dict[view_template_file_name]['dataset_name'],
            destination_dataset=dataset_view_data['dataset_name'],
        )
        result_list.append(result)
        total_bytes_processed += (result.total_bytes_processed or 0)
        total_rows += (result.total_rows or 0)
    duration = time.perf_counter() - start
    LOGGER.info(
        (
            'materialized views, number of views: %d,'
            ' total rows: %s, %s bytes processed, took: %.3fs (%0.3fs / views)'
        ),
        len(materialized_view_dict),
        total_rows,
        total_bytes_processed,
        duration,
        duration / len(materialized_view_dict),
    )
    return MaterializeViewListResult(result_list)


@dataclass(frozen=True)
class MaterializeViewState:
    project: str
    dataset: str
    full_view_dependencies: Mapping[str, Set[str]]
    last_modified_timestamp_map: dict[str, datetime]

    def get_timestamp_or_none(self, full_table_or_view_name: str) -> Optional[datetime]:
        return self.last_modified_timestamp_map.get(full_table_or_view_name)

    def get_timestamp(self, full_table_or_view_name: str) -> datetime:
        return self.last_modified_timestamp_map[full_table_or_view_name]

    def update_timestamp(self, full_table_or_view_name: str, timestamp: datetime):
        self.last_modified_timestamp_map[full_table_or_view_name] = timestamp

    def get_full_view_name(self, view_name: str) -> str:
        return f'{self.project}.{self.dataset}.{view_name}'

    def get_latest_timestamp_of_view_and_dependencies(  # pylint: disable=useless-return
        self,
        full_view_name: str
    ) -> datetime:
        if full_view_name not in self.full_view_dependencies:
            raise KeyError(f'View {repr(full_view_name)} not in view dependencies')
        dependencies = self.full_view_dependencies[full_view_name]
        view_dependencies = {
            dependency
            for dependency in dependencies
            if dependency in self.full_view_dependencies
        }
        direct_timestamps = {
            self.get_timestamp(dependency)
            for dependency in dependencies
        }
        indirect_timestamps = {
            self.get_latest_timestamp_of_view_and_dependencies(view_dependency)
            for view_dependency in view_dependencies
        }
        view_timestamp = self.get_timestamp(full_view_name)
        return max({view_timestamp} | direct_timestamps | indirect_timestamps)


def materialize_views_if_necessary_with_state(  # pylint: disable=too-many-locals,too-many-arguments
    client: bigquery.Client,
    view_list_config: ViewListConfig,
    state: MaterializeViewState,
    selected_view_names: Optional[Container[str]] = None
) -> MaterializeViewListResult:
    LOGGER.debug('state: %r', state)
    start = time.perf_counter()
    total_bytes_processed = 0
    total_rows = 0
    result_list = []
    for view_config in view_list_config:
        if (
            not view_config.is_materialized()
            or (selected_view_names and view_config.view_name not in selected_view_names)
        ):
            continue
        latest_timestamp_of_view_and_dependencies = (
            state.get_latest_timestamp_of_view_and_dependencies(
                state.get_full_view_name(view_config.view_name)
            )
        )
        LOGGER.info(
            'latest_timestamp_of_view_and_dependencies (view name: %r): %r',
            view_config.view_name,
            latest_timestamp_of_view_and_dependencies.isoformat()
        )
        destination_dataset_and_table_dict = view_config.get_destination_dataset_and_table_name(
            state.dataset
        )
        destination_dataset = destination_dataset_and_table_dict['dataset_name']
        destination_table_name = destination_dataset_and_table_dict['table_name']
        full_destination_table_name = view_config.get_full_destination_table_name(
            project=state.project,
            dataset=state.dataset
        )
        LOGGER.debug('full_destination_table_name: %r', full_destination_table_name)
        destination_table_timestamp = state.get_timestamp_or_none(full_destination_table_name)
        LOGGER.info(
            'destination_table_timestamp (table name: %r): %r',
            full_destination_table_name,
            (
                destination_table_timestamp.isoformat()
                if destination_table_timestamp
                else None
            )
        )
        if not destination_table_timestamp:
            LOGGER.warning(
                'No timestamp found for destination table: %r',
                full_destination_table_name
            )
        if (
            destination_table_timestamp
            and destination_table_timestamp > latest_timestamp_of_view_and_dependencies
        ):
            LOGGER.info('Skipping materialization, destination table already up-to-date')
            continue
        result = materialize_view(
            client=client,
            project=state.project,
            source_dataset=state.dataset,
            source_view_name=view_config.view_name,
            destination_dataset=destination_dataset,
            destination_table_name=destination_table_name
        )
        state.update_timestamp(full_destination_table_name, get_current_timestamp())
        result_list.append(result)
        total_bytes_processed += (result.total_bytes_processed or 0)
        total_rows += (result.total_rows or 0)
    duration = time.perf_counter() - start
    if result_list:
        LOGGER.info(
            (
                'materialized views, number of views: %d, '
                'total rows: %s, '
                '%s bytes processed, '
                'took: %.3fs (%0.3fs / views)'
            ),
            len(result_list),
            total_rows,
            total_bytes_processed,
            duration,
            duration / len(result_list),
        )
    else:
        LOGGER.info('There are no views to materialize.')
    return MaterializeViewListResult(result_list)


def materialize_views_if_necessary(  # pylint: disable=too-many-locals
    client: bigquery.Client,
    project: str,
    dataset: str,
    view_list_config: ViewListConfig,
    selected_view_names: Optional[Container[str]] = None
) -> MaterializeViewListResult:
    view_dependencies = get_view_dependencies(
        client=client,
        project=project,
        dataset=dataset
    )
    LOGGER.info(
        'view_dependencies:\n```json\n%s\n```',
        get_json(view_dependencies)
    )
    flat_view_dependencies = get_flat_view_dependencies(view_dependencies)
    LOGGER.info('flat_view_dependencies: %r', flat_view_dependencies)
    full_destination_table_names = {
        view_config.get_full_destination_table_name(
            project=project,
            dataset=dataset
        )
        for view_config in view_list_config
        if view_config.is_materialized()
    }
    LOGGER.info('full_destination_table_names: %r', full_destination_table_names)
    last_modified_timestamp_by_full_table_or_view_name_map = (
        get_last_modified_timestamp_by_full_table_or_view_name_map(
            client=client,
            table_or_view_names=(
                flat_view_dependencies | full_destination_table_names
            )
        )
    )
    LOGGER.info(
        'last_modified_timestamp_by_full_table_or_view_name_map:\n```json\n%s\n```',
        get_json(last_modified_timestamp_by_full_table_or_view_name_map)
    )
    state = MaterializeViewState(
        project=project,
        dataset=dataset,
        full_view_dependencies=view_dependencies,
        last_modified_timestamp_map=dict(last_modified_timestamp_by_full_table_or_view_name_map)
    )
    return materialize_views_if_necessary_with_state(
        client=client,
        view_list_config=view_list_config,
        state=state,
        selected_view_names=selected_view_names
    )
