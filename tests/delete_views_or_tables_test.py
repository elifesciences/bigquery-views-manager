from unittest.mock import MagicMock
from typing import List
from collections import OrderedDict

from google.cloud import bigquery

from bigquery_views_manager.delete_views_or_tables import delete_views_or_tables

from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY


PROJECT_1 = "project1"
DATASET_1 = "dataset1"
TABLE_1 = "table1"


def get_input_ordered_dict_view_mapping():
    view_mapping = OrderedDict()
    view_mapping[TABLE_1] = {
        DATASET_NAME_KEY: DATASET_1,
        VIEW_OR_TABLE_NAME_KEY: TABLE_1,
    }
    return view_mapping


def _to_mock_table_list_item(table_name: str) -> bigquery.table.TableListItem:
    mock = MagicMock()
    mock.table_id = table_name
    return mock


def _to_mock_table_list_items(
        table_names: List[str]
) -> List[bigquery.table.TableListItem]:
    return [_to_mock_table_list_item(table_name) for table_name in table_names]


class TestDeleteViewsOrTables:
    def test_should_call_delete_table_if_table_in_dataset(self, bq_client):
        delete_views_or_tables(bq_client, get_input_ordered_dict_view_mapping())

        bq_client.dataset.assert_called_with(DATASET_1)
        dataset_ref = bq_client.dataset.return_value

        dataset_ref.table.assert_called_with(TABLE_1)
        table_ref = dataset_ref.table.return_value

        bq_client.delete_table.assert_called_with(table_ref)
