from pathlib import Path
from collections import OrderedDict
from unittest.mock import patch, MagicMock

import pytest

from bigquery_views_manager.view_list import (
    DATASET_NAME_KEY,
    VIEW_OR_TABLE_NAME_KEY,
    load_view_list_config
)

import bigquery_views_manager.cli as target_module
from bigquery_views_manager.cli import (
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


@pytest.fixture(name='delete_views_or_tables_mock', autouse=True)
def _delete_views_or_tables_mock():
    with patch.object(target_module, 'delete_views_or_tables') as mock:
        yield mock


@pytest.fixture(name='materialize_views_mock', autouse=True)
def _materialize_views_mock():
    with patch.object(target_module, 'materialize_views') as mock:
        yield mock


@pytest.fixture(name='diff_views_mock', autouse=True)
def _diff_views_mock():
    with patch.object(target_module, 'diff_views') as mock:
        yield mock


@pytest.fixture(name='get_views_mock', autouse=True)
def _get_views_mock():
    with patch.object(target_module, 'get_views') as mock:
        yield mock


@pytest.fixture(name='get_bq_view_names_mock', autouse=True)
def _get_bq_view_names_mock():
    with patch.object(target_module, 'get_bq_view_names') as mock:
        yield mock


def get_ordered_dict_view_mapping():
    result = OrderedDict()
    result["view1"] = {DATASET_NAME_KEY: "dataset1", VIEW_OR_TABLE_NAME_KEY: "view1"}
    result["view2"] = {DATASET_NAME_KEY: "dataset2", VIEW_OR_TABLE_NAME_KEY: "view2"}
    return result


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


class TestDeleteViewsSubCommand:
    def test_should_create_simple_view(
            self,
            temp_dir: Path,
            delete_views_or_tables_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1'
        ]))
        main([
            'delete-views',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        delete_views_or_tables_mock.assert_called()


class TestMaterializeViewsSubCommand:
    def test_should_materialize_simple_view(
            self,
            temp_dir: Path,
            materialize_views_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true'
        ]))
        main([
            'materialize-views',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        materialize_views_mock.assert_called()


class TestDeleteMaterializedTablesSubCommand:
    def test_should_delete_materialized_tables(
            self,
            temp_dir: Path,
            delete_views_or_tables_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true'
        ]))
        main([
            'delete-materialized-tables',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        delete_views_or_tables_mock.assert_called()


class TestDiffViewsSubCommand:
    def test_should_call_diff_views(
            self,
            temp_dir: Path,
            diff_views_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true'
        ]))
        main([
            'diff-views',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        diff_views_mock.assert_called()


class TestGetViewsSubCommand:
    def test_should_call_get_views(
            self,
            temp_dir: Path,
            get_views_mock: MagicMock):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true'
        ]))
        main([
            'get-views',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        get_views_mock.assert_called()


class TestSortViewListSubCommand:
    def test_should_sort_view_list(
            self,
            temp_dir: Path):
        view_config_path = temp_dir / 'views.yml'
        view_config_path.write_text('\n'.join([
            '- view1',
            '- view2:',
            '    materialize: true'
        ]))
        (temp_dir / 'view1.sql').write_text(
            'SELECT * FROM `{project}.{dataset}.mview2`'
        )
        (temp_dir / 'view2.sql').write_text(
            'SELECT 1'
        )
        main([
            'sort-view-list',
            '--dataset=dataset1',
            '--view-list-config=%s' % view_config_path
        ])
        assert load_view_list_config(view_config_path).view_names == [
            'view2',
            'view1'
        ]
