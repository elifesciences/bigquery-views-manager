from datetime import datetime
import json
from typing import Any


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):  # pylint: disable=arguments-renamed
        if isinstance(obj, set):
            return list(sorted(obj))
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def get_json(obj: Any):
    return json.dumps(obj, cls=JsonEncoder, indent=2)
