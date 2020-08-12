import logging
from collections import OrderedDict
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

from .view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

LOGGER = logging.getLogger(__name__)


def _get_existing_table_names(client: bigquery.Client, dataset: str):
    return [
        table_item.table_id
        for table_item in client.list_tables(dataset=dataset)
    ]


def does_bigquery_table_exist(client: bigquery.Client, dataset_name: str,
                              table_name: str):
    dataset_ref = client.dataset(dataset_name)
    table_ref = dataset_ref.table(table_name)

    try:
        client.get_table(table_ref)
        return True
    except NotFound:
        return False


def delete_views_or_table(client: bigquery.Client, view_or_table_name: str,
                          dataset: str):
    LOGGER.debug("delete_views_or_tables: %s", view_or_table_name)
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(view_or_table_name)
    client.delete_table(table_ref)
    LOGGER.info("deleted view or table: %s", view_or_table_name)


def delete_views_or_tables(client: bigquery.Client,
                           view_template_to_table_name_mapping: OrderedDict):
    LOGGER.debug("delete_views_or_tables: %s",
                 view_template_to_table_name_mapping)
    for _, dataset_view_or_table_data in view_template_to_table_name_mapping.items():
        if does_bigquery_table_exist(
                client,
                table_name=dataset_view_or_table_data.get(
                    VIEW_OR_TABLE_NAME_KEY),
                dataset_name=dataset_view_or_table_data.get(DATASET_NAME_KEY),
        ):
            delete_views_or_table(
                client,
                dataset_view_or_table_data.get(VIEW_OR_TABLE_NAME_KEY),
                dataset=dataset_view_or_table_data.get(DATASET_NAME_KEY),
            )
