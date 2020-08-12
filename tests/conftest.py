from unittest.mock import MagicMock

import pytest


@pytest.fixture(name="bq_client")
def _bq_client():
    return MagicMock()
