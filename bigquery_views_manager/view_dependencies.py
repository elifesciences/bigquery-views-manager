from typing import Mapping, Sequence
from google.cloud import bigquery


def get_view_definition_query(
    project: str,
    dataset: str,
) -> str:
    return (
        'SELECT table_name, view_definition\n'
        f'FROM `{project}.{dataset}.INFORMATION_SCHEMA.VIEWS`'
    )


def get_view_dependencies(
    client: bigquery.Client,
    project: str,
    dataset: str
) -> Mapping[str, Sequence[str]]:
    query_job = client.query(get_view_definition_query(
        project=project,
        dataset=dataset
    ))
    query_job.result()
    return {}
