import logging
import time
from collections import OrderedDict
from collections.abc import Container
from itertools import islice
from dataclasses import dataclass
from typing import Optional, Sequence

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig

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


def get_select_all_from_query(
    view_name: str,
    project: str,
    dataset: str
) -> str:
    return f"SELECT * FROM `{project}.{dataset}.{view_name}`"


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
        "materializing view: %s.%s -> %s.%s",
        source_dataset,
        source_view_name,
        destination_dataset,
        destination_table_name
    )
    LOGGER.debug("materialize_view: %s=%s", destination_table_name, [query])

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
        LOGGER.debug("sample_result: %s", sample_result)
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
    LOGGER.info("view_names: %s", materialized_view_dict)
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


def materialize_views_if_necessary(
    client: bigquery.Client,
    project: str,
    dataset: str,
    view_list_config: ViewListConfig,
    selected_view_names: Optional[Container[str]] = None
) -> MaterializeViewListResult:
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
        destination_dataset_and_table_dict = view_config.get_destination_dataset_and_table_name(
            dataset
        )
        result = materialize_view(
            client=client,
            project=project,
            source_dataset=dataset,
            source_view_name=view_config.view_name,
            destination_dataset=destination_dataset_and_table_dict['dataset_name'],
            destination_table_name=destination_dataset_and_table_dict['table_name']
        )
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
