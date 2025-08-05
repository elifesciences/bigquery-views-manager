from unittest.mock import MagicMock

from google.cloud.bigquery.table import Row

from bigquery_views_manager.utils.bigquery import iter_dict_from_bq_query


class TestIterDictFromBqQuery:
    def test_should_return_dict_for_row(self, bq_client: MagicMock):
        mock_query_job = bq_client.query.return_value
        mock_query_job.result.return_value = [
            Row(['value1', 'value2'], {'key1': 0, 'key2': 1})
        ]
        result = list(iter_dict_from_bq_query(
            client=bq_client,
            query='query1'
        ))
        assert result == [{
            'key1': 'value1',
            'key2': 'value2'
        }]
