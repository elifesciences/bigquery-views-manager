from collections.abc import Set
import logging
import re
from typing import Mapping

from google.cloud import bigquery

import bigquery_views_manager.utils.bigquery as bigquery_utils


LOGGER = logging.getLogger(__name__)


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
