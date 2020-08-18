import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from py._path.local import LocalPath


@pytest.fixture(scope='session', autouse=True)
def setup_logging():
    logging.basicConfig(level='INFO')
    for name in ['tests', 'bigquery_views_manager']:
        logging.getLogger(name).setLevel('DEBUG')


@pytest.fixture(name="bq_client")
def _bq_client():
    return MagicMock()


@pytest.fixture()
def temp_dir(tmpdir: LocalPath) -> Path:
    return Path(tmpdir)
