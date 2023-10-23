import logging
import time
from collections import OrderedDict
from itertools import islice
from dataclasses import dataclass
from typing import Optional, Sequence

from google.cloud import bigquery
from google.cloud.bigquery.job import QueryJobConfig

from .view_list import VIEW_OR_TABLE_NAME_KEY, DATASET_NAME_KEY

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


def get_select_all_from_query(view_name: str, project: str,
                              dataset: str) -> str:
    return f"SELECT * FROM `{project}.{dataset}.{view_name}`"


def materialize_view(  # pylint: disable=too-many-arguments, too-many-locals
        client: bigquery.Client,
        source_view_name: str,
        destination_table_name: str,
        project: str,
        source_dataset: str,
        destination_dataset: str,
) -> MaterializeViewResult:
    query = get_select_all_from_query(source_view_name,
                                      project=project,
                                      dataset=source_dataset)
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
        materialized_view_dict: OrderedDict,
        source_view_dict: OrderedDict,
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
            source_view_name=source_view_dict.get(view_template_file_name).get(
                VIEW_OR_TABLE_NAME_KEY),
            destination_table_name=dataset_view_data.get(
                VIEW_OR_TABLE_NAME_KEY),
            project=project,
            source_dataset=source_view_dict.get(view_template_file_name).get(
                DATASET_NAME_KEY),
            destination_dataset=dataset_view_data.get(DATASET_NAME_KEY),
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
