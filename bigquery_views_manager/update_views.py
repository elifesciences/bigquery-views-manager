import logging
from collections import OrderedDict

from google.cloud import bigquery

from .views import get_local_view_query
from .materialize_views import materialize_view
from .view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

LOGGER = logging.getLogger(__name__)


def get_create_or_replace_view_query(view: bigquery.Table) -> str:
    return (
        f"CREATE OR REPLACE VIEW {view.dataset_id}.{view.table_id} AS {view.view_query}"
    )


def update_or_create_view(client: bigquery.Client, view_name: str,
                          view_query: str, dataset: str):
    LOGGER.debug("update_view: %s=%s", view_name, [view_query])
    dataset_ref = client.dataset(dataset)
    view_ref = dataset_ref.table(view_name)
    view = bigquery.Table(view_ref)
    view.view_query = view_query

    query_job = client.query(get_create_or_replace_view_query(view))
    query_job.result()  # wait for query job to finish

    updated_view = client.get_table(view)
    LOGGER.info("updated or replaced view: %s", updated_view.full_table_id)
    LOGGER.debug("view schema (%s): %s", updated_view.full_table_id,
                 updated_view.schema)


def update_or_create_views(  # pylint: disable=too-many-arguments
        client: bigquery.Client,
        base_dir: str,
        view_names_dict: OrderedDict,
        materialized_view_names: OrderedDict,
        project: str,
        default_dataset: str,
        view_to_dataset_mapping: dict,
):
    LOGGER.info("view_names: %s (materialize: %s)", view_names_dict,
                materialized_view_names)
    for view_template_file_name, dataset_view_data in view_names_dict.items():
        view_query = get_local_view_query(
            base_dir,
            view_template_file_name,
            project=project,
            default_dataset=default_dataset,
            view_to_dataset_mapping=view_to_dataset_mapping,
        )
        view_name = dataset_view_data.get(VIEW_OR_TABLE_NAME_KEY)
        dataset_name = dataset_view_data.get(DATASET_NAME_KEY)
        update_or_create_view(client,
                              view_name,
                              view_query,
                              dataset=dataset_name)
        if view_template_file_name in materialized_view_names.keys():
            materialize_view(
                client,
                source_view_name=view_name,
                destination_table_name=materialized_view_names.get(
                    view_template_file_name).get(VIEW_OR_TABLE_NAME_KEY),
                project=project,
                destination_dataset=materialized_view_names.get(
                    view_template_file_name).get(DATASET_NAME_KEY),
                source_dataset=dataset_name,
            )
