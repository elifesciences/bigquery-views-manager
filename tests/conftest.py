import logging
from pathlib import Path
from typing import Iterator
from unittest.mock import MagicMock, patch

import pytest

import bigquery_views_manager.utils.bigquery as bigquery_utils


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logging.basicConfig(level='INFO')
    for name in ['tests', 'bigquery_views_manager']:
        logging.getLogger(name).setLevel('DEBUG')


@pytest.fixture(name="bq_client")
def _bq_client():
    return MagicMock()


@pytest.fixture(name='iter_dict_from_bq_query_mock', autouse=True)
def _iter_dict_from_bq_query_mock() -> Iterator[MagicMock]:
    with patch.object(bigquery_utils, 'iter_dict_from_bq_query') as mock:
        yield mock


@pytest.fixture()
def temp_dir(tmp_path: Path) -> Path:
    return tmp_path
