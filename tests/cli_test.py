from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch, MagicMock

import pytest

from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

import bigquery_views_manager.cli as target_module
from bigquery_views_manager.cli import (
    load_view_mapping,
    main
)


VIEW_1 = "view1,dataset1"
VIEW_2 = "view2,dataset2"


@pytest.fixture(name='bigquery_mock', autouse=True)
def _bigquery_mock():
    with patch.object(target_module, 'bigquery') as mock:
        yield mock


@pytest.fixture(name='update_or_create_views_mock', autouse=True)
def _update_or_create_views_mock():
    with patch.object(target_module, 'update_or_create_views') as mock:
        yield mock


def get_ordered_dict_view_mapping():
    result = OrderedDict()
    result["view1"] = {DATASET_NAME_KEY: "dataset1", VIEW_OR_TABLE_NAME_KEY: "view1"}
    result["view2"] = {DATASET_NAME_KEY: "dataset2", VIEW_OR_TABLE_NAME_KEY: "view2"}
    return result


class TestLoadViewList:
    def test_should_load_list_of_views_as_ordered_dict(self, tmpdir):
        views_file = tmpdir.join("views.lst")
        views_file.write("\n".join([VIEW_1, VIEW_2]))
        assert (
            load_view_mapping(filename=views_file, should_map_table=True,
                              default_dataset_name="dataset1",
                              is_materialized_view=False) == get_ordered_dict_view_mapping()
        )


class TestCreateOrReplaceViewsSubCommand:
    def test_should_create_simple_view(
            self,
            temp_dir: Path,
            update_or_create_views_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1'
        ]))
        main([
            'create-or-replace-views',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        update_or_create_views_mock.assert_called()
