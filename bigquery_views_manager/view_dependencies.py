from typing import Mapping, Sequence
from google.cloud import bigquery

from bigquery_views_manager.view_list import ViewListConfig


def get_view_dependencies(
    client: bigquery.Client,
    project: str,
    dataset: str,
    view_list_config: ViewListConfig,
) -> Mapping[str, Sequence[str]]:
    return {}
