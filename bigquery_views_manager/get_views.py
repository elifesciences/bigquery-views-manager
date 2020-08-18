import logging
from collections import OrderedDict

from google.cloud import bigquery

from .views import get_bq_view_query, get_view_template_file
from .view_template import ViewTemplate
from .view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

LOGGER = logging.getLogger(__name__)


def get_view(  # pylint: disable=too-many-arguments
        client: bigquery.Client,
        base_dir: str,
        view_name: str,
        view_template_name: str,
        project: str,
        dataset: str,
):
    bq_view_query = get_bq_view_query(client, view_name, dataset=dataset)

    LOGGER.debug("bq_view_query(%s)=%r", view_name, bq_view_query)
    view_template = ViewTemplate.from_query(bq_view_query, project=project).normalized
    LOGGER.debug("view_template(%s)=%r", view_name, view_template)
    view_template_file = get_view_template_file(base_dir, view_template_name)
    view_template.to_file(view_template_file)
    LOGGER.info("updated %s", view_template_file)


def get_views(
        client: bigquery.Client,
        base_dir: str,
        view_names_ordered_dict: OrderedDict,
        project: str,
):
    LOGGER.debug("view_names: %s", view_names_ordered_dict)
    for (
            view_template_name,
            dataset_table_or_view_data,
    ) in view_names_ordered_dict.items():
        get_view(
            client,
            base_dir,
            dataset_table_or_view_data.get(VIEW_OR_TABLE_NAME_KEY),
            view_template_name,
            project=project,
            dataset=dataset_table_or_view_data.get(DATASET_NAME_KEY),
        )
