from collections.abc import Set
from dataclasses import dataclass
from datetime import datetime
import logging
import re
import textwrap
from typing import Mapping

from google.cloud import bigquery

import bigquery_views_manager.utils.bigquery as bigquery_utils


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetRef:
    project: str
    dataset: str


def get_view_definition_query(
    project: str,
    dataset: str
) -> str:
    return (
        'SELECT table_name, view_definition\n'
        f'FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`'
    )


def get_view_definition_map(
    client: bigquery.Client,
    project: str,
    dataset: str
) -> Mapping[str, str]:
    query_result_dict_iterable = bigquery_utils.iter_dict_from_bq_query(
        client=client,
        query=get_view_definition_query(
            project=project,
            dataset=dataset
        )
    )
    return {
        result_dict['table_name']: result_dict['view_definition']
        for result_dict in query_result_dict_iterable
    }


def get_view_dependencies_from_view_definition(
    view_definition: str
) -> Set[str]:
    return set(re.findall(
        r'\b(?:FROM|JOIN)\s+`?((?:[a-zA-Z0-9\-_.]+\.){2}[a-zA-Z0-9_.]+)`?',
        view_definition,
        re.IGNORECASE
    ))


def get_view_dependencies(
    client: bigquery.Client,
    project: str,
    dataset: str
) -> Mapping[str, Set[str]]:
    view_definition_map = get_view_definition_map(
        client=client,
        project=project,
        dataset=dataset
    )
    LOGGER.debug('view_definition_map: %r', view_definition_map)
    return {
        view_name: get_view_dependencies_from_view_definition(view_definition)
        for view_name, view_definition in view_definition_map.items()
    }


def get_flat_view_dependencies(
    view_dependencies: Mapping[str, Set[str]]
) -> Set[str]:
    LOGGER.debug('view_dependencies: %r', view_dependencies)
    return {
        value
        for values in view_dependencies.values()
        for value in values
    }


def get_table_or_view_last_modified_timestamp_query(
    project: str,
    dataset: str
) -> str:
    return textwrap.dedent(
        f'''
        SELECT
            project_id,
            dataset_id,
            table_id,
            TIMESTAMP_MILLIS(last_modified_time) AS last_modified_timestamp
        FROM `{project}.{dataset}.__TABLES__`
        '''
    )


def get_table_or_view_last_modified_timestamp_query_for_multiple_dataset_refs(
    dataset_refs: Set[DatasetRef]
) -> str:
    assert dataset_refs
    dataset_ref = list(dataset_refs)[0]
    return get_table_or_view_last_modified_timestamp_query(
        project=dataset_ref.project, dataset=dataset_ref.dataset
    )


def get_dataset_ref_for_full_table_or_view_name(
    full_view_or_table_name: str
) -> DatasetRef:
    project, dataset, _table_or_view_name = full_view_or_table_name.split('.')
    return DatasetRef(project=project, dataset=dataset)


def get_last_modified_timestamp_by_full_table_or_view_name(
    table_or_view_names: Set[str]
) -> Mapping[str, datetime]:
    LOGGER.debug('table_or_view_names: %r', table_or_view_names)
    dataset_refs = {
        get_dataset_ref_for_full_table_or_view_name(full_table_or_view_name)
        for full_table_or_view_name in table_or_view_names
    }
    LOGGER.info('dataset_refs: %r', dataset_refs)
    return {}
