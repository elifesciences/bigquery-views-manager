from typing import Mapping, Sequence

from google.cloud import bigquery

import bigquery_views_manager.utils.bigquery as bigquery_utils


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


def get_view_dependencies(
    client: bigquery.Client,
    project: str,
    dataset: str
) -> Mapping[str, Sequence[str]]:
    get_view_definition_map(
        client=client,
        project=project,
        dataset=dataset
    )
    return {}
