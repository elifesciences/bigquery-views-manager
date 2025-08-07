from datetime import datetime
import json

from bigquery_views_manager.utils.json import get_json


TIMESTAMP_1 = datetime.fromisoformat('2001-01-01T00:00:00+00:00')


class TestGetJson:
    def test_should_format_view_dependencies_with_set_as_list(self):
        assert json.loads(get_json({
            'view_1': {'table_1', 'table_2'}
        })) == {
            'view_1': ['table_1', 'table_2']
        }

    def test_should_format_datetime(self):
        assert get_json(TIMESTAMP_1) == json.dumps(TIMESTAMP_1.isoformat())
