from pathlib import Path
from typing import Dict

from google.cloud import bigquery

from .view_template import ViewTemplate


def get_bq_view_names(client: bigquery.Client, dataset: str):
    return [
        table.table_id for table in client.list_tables(dataset=dataset)
        if table.table_type == "VIEW"
    ]


def get_bq_view_query(client: bigquery.Client, view_name: str, dataset: str):
    dataset_ref = client.dataset(dataset)
    view_ref = dataset_ref.table(view_name)
    view = client.get_table(view_ref)
    return view.view_query


def get_view_template_file(base_dir: str, view_file_name: str) -> str:
    return Path(base_dir).joinpath("%s.sql" % view_file_name)


def get_local_view_template(base_dir: str,
                            view_template_file_name: str) -> ViewTemplate:
    view_template_file = get_view_template_file(base_dir,
                                                view_template_file_name)
    return ViewTemplate.from_file(view_template_file)


def get_local_view_query(
        base_dir: str,
        view_template_file_name: str,
        project: str,
        default_dataset: str,
        view_to_dataset_mapping: Dict[str, str],
) -> str:
    view_template = get_local_view_template(base_dir, view_template_file_name)
    return view_template.substitute(
        project=project,
        default_dataset=default_dataset,
        view_to_dataset_mapping=view_to_dataset_mapping,
    )
