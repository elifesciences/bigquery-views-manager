import argparse
import logging
import sys
from pathlib import Path
from typing import Dict, List
from abc import ABCMeta, abstractmethod
from collections import OrderedDict

from google.cloud import bigquery

from .views import get_bq_view_names
from .view_list import (
    get_mapped_materialized_view_subset,
    extend_or_subset_mapped_view_subset,
    map_view_to_dataset_from_template_mapping_dict,
    create_simple_view_mapping_from_view_list,
    load_view_list_config,
    save_view_list_config,
    ViewConfig
)

from .update_views import update_or_create_views
from .materialize_views import materialize_views
from .diff_views import diff_views
from .get_views import get_views
from .delete_views_or_tables import delete_views_or_tables
from .config_tables import get_local_config_table_names, update_or_create_config_tables

from . import configure_warnings  # noqa pylint: disable=unused-import


LOGGER = logging.getLogger(__name__)

DEFAULT_VIEW_LIST_CONFIG_FILE = "views/views.yml"
DEFAULT_VIEW_LIST_FILE = "views/views.lst"
DEFAULT_MATERIALIZED_VIEW_LIST_FILE = "views/materialized-views.lst"

DEFAULT_CONFIG_TABLES_BASE_DIR = "config-tables"


def add_view_list_config_file_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--view-list-config",
        type=str,
        default=DEFAULT_VIEW_LIST_CONFIG_FILE,
        help="Path to view list config (yaml)",
    )


def add_view_list_file_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--view-list-file",
        type=str,
        default=DEFAULT_VIEW_LIST_FILE,
        help="Path to view list file",
    )


def add_materialized_view_list_file_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--materialized-view-list-file",
        type=str,
        default=DEFAULT_MATERIALIZED_VIEW_LIST_FILE,
        help="Path to materialized view list file",
    )


def add_view_names_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        dest="view_names", metavar="view-names", nargs="*", help="View names"
    )


def add_config_tables_base_dir_file_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--config-tables-base-dir",
        type=str,
        default=DEFAULT_CONFIG_TABLES_BASE_DIR,
        help="Path to directory with config tables",
    )


def add_table_names_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        dest="table_names", metavar="table-names", nargs="*", help="Table names"
    )


def disable_view_name_mapping_argument(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--disable-view-name-mapping",
        action="store_true",
        help="Do not use mapping defined in the view and materialized view list file",
    )


class SubCommand(metaclass=ABCMeta):
    def __init__(self, name, description):
        self.name = name
        self.description = description

    @abstractmethod
    def add_arguments(self, parser: argparse.ArgumentParser):
        pass

    @abstractmethod
    def run(self, client: bigquery.Client, args: argparse.Namespace):
        pass


class CreateOrReplaceViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__("create-or-replace-views", "Create or Replace Views")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)
        parser.add_argument(
            "--materialize",
            action="store_true",
            help="Materialize views in the materialized view list while updating",
        )

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        view_list_config = load_view_list_config(
            args.view_list_config
        ).resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        views_ordered_dict_all = view_list_config.to_views_ordered_dict(
            args.dataset
        )
        LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)
        materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
            args.dataset
        )
        LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

        views_dict = (
            extend_or_subset_mapped_view_subset(
                views_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else views_ordered_dict_all
        )
        LOGGER.debug('views_dict: %s', views_dict)

        materialized_view_ordered_dict = (
            get_mapped_materialized_view_subset(
                materialized_view_ordered_dict_all, set(views_dict.keys())
            )
            if args.materialize
            else OrderedDict()
        )

        # what is the dataset of a (materialized) view - used in re-writing template
        # ASSUMPTION : view names are not shared across dataset,
        # and in case they are, the last view name in the materialized view list is used
        view_to_dataset_mapping = map_view_to_dataset_from_template_mapping_dict(
            views_ordered_dict_all
        )
        view_to_dataset_mapping.update(
            map_view_to_dataset_from_template_mapping_dict(
                materialized_view_ordered_dict_all
            )
        )

        update_or_create_views(
            client,
            Path(args.view_list_config).parent,
            views_dict,
            materialized_view_names=materialized_view_ordered_dict,
            project=client.project,
            default_dataset=args.dataset,
            view_to_dataset_mapping=view_to_dataset_mapping,
        )


class DeleteViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__("delete-views", "Delete Views")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        view_list_config = load_view_list_config(
            args.view_list_config
        ).resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        views_ordered_dict_all = view_list_config.to_views_ordered_dict(
            args.dataset
        )
        LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)

        views_dict = (
            extend_or_subset_mapped_view_subset(
                views_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else views_ordered_dict_all
        )
        delete_views_or_tables(client, views_dict)


class MaterializeViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__("materialize-views", "Materialize Views")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        view_list_config = load_view_list_config(
            args.view_list_config
        ).resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        views_ordered_dict_all = view_list_config.to_views_ordered_dict(
            args.dataset
        )
        LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)
        materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
            args.dataset
        )
        LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

        materialized_view_ordered_dict = (
            get_mapped_materialized_view_subset(
                materialized_view_ordered_dict_all, args.view_names
            )
            if args.view_names
            else materialized_view_ordered_dict_all
        )

        materialize_views(
            client,
            materialized_view_dict=materialized_view_ordered_dict,
            source_view_dict=views_ordered_dict_all,
            project=client.project,
        )


class DeleteMaterializedTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__("delete-materialized-tables", "Delete Materialized Tables")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        view_list_config = load_view_list_config(
            args.view_list_config
        ).resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
            args.dataset
        )
        LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

        materialized_view_ordered_dict = (
            extend_or_subset_mapped_view_subset(
                materialized_view_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else materialized_view_ordered_dict_all
        )
        delete_views_or_tables(client, materialized_view_ordered_dict)


class DiffViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__("diff-views", "Show difference between local and remote views")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)
        parser.add_argument(
            "--fail-if-changed", action="store_true", help="Fail if changed"
        )

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        view_list_config = load_view_list_config(
            args.view_list_config
        ).resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        views_ordered_dict_all = view_list_config.to_views_ordered_dict(
            args.dataset
        )
        LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)
        materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
            args.dataset
        )
        LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

        views_dict = (
            extend_or_subset_mapped_view_subset(
                views_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else views_ordered_dict_all
        )

        # what is the dataset of a (materialized) view - used in re-writing template
        # ASSUMPTION : view names are not shared across dataset,
        # and in case they are, the last view name in the materialized view list is used
        view_to_dataset_mapping = map_view_to_dataset_from_template_mapping_dict(
            views_ordered_dict_all
        )
        view_to_dataset_mapping.update(
            map_view_to_dataset_from_template_mapping_dict(
                materialized_view_ordered_dict_all
            )
        )

        has_changed = diff_views(
            client,
            Path(args.view_list_config).parent,
            views_dict,
            project=client.project,
            default_dataset=args.dataset,
            view_to_dataset_mapping=view_to_dataset_mapping,
        )
        if has_changed and args.fail_if_changed:
            sys.exit(2)


class GetViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "get-views",
            (
                "Copy remote views to local repository.",
                ' Placeholders will be added for matching "project.dataset." prefixes.',
            ),
        )

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)
        add_view_names_argument(parser)
        parser.add_argument(
            "--all-remote-views",
            action="store_true",
            help="Ignore view list and get all remote views from BigQuery",
        )
        parser.add_argument(
            "--add-to-view-list",
            action="store_true",
            help="Add any missing views to the view list",
        )
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        original_view_list_config = load_view_list_config(
            args.view_list_config
        )
        view_list_config = original_view_list_config.resolve_conditions({
            'project': client.project,
            'dataset': args.dataset
        })
        LOGGER.info('view_list_config: %s', view_list_config)
        views_ordered_dict_all = view_list_config.to_views_ordered_dict(
            args.dataset
        )
        LOGGER.debug('views_ordered_dict_all: %s', views_ordered_dict_all)
        materialized_view_ordered_dict_all = view_list_config.to_materialized_view_ordered_dict(
            args.dataset
        )
        LOGGER.debug('materialized_view_ordered_dict_all: %s', materialized_view_ordered_dict_all)

        if args.all_remote_views:
            view_names = get_bq_view_names(client, dataset=args.dataset)
            views_dict = create_simple_view_mapping_from_view_list(
                args.dataset, view_names
            )
        else:
            views_dict = (
                extend_or_subset_mapped_view_subset(
                    views_ordered_dict_all, args.view_names, args.dataset
                )
                if args.view_names
                else views_ordered_dict_all
            )

        base_dir = Path(args.view_list_config).parent
        get_views(client, base_dir, views_dict, project=client.project)

        if args.add_to_view_list:
            updated_view_list_config = original_view_list_config
            for view_name in views_dict.keys():
                if not view_list_config.has_view(view_name):
                    updated_view_list_config = updated_view_list_config.add_view(
                        ViewConfig(view_name)
                    )
            updated_view_list_config = updated_view_list_config.sort_insert_order(base_dir)
            save_view_list_config(
                updated_view_list_config,
                args.view_list_config
            )


class SortViewListSubCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "sort-view-list",
            (
                "Sort the view list according to the correct insert order.",
                " (corresponding view SQL template files need to be present)",
            ),
        )

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_config_file_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        base_dir = Path(args.view_list_config).parent
        view_list_config = load_view_list_config(
            args.view_list_config
        )
        LOGGER.info('view_list_config: %s', view_list_config)

        sorted_view_list_config = view_list_config.sort_insert_order(base_dir)
        LOGGER.info('sorted_view_list_config: %s', sorted_view_list_config)

        save_view_list_config(
            sorted_view_list_config,
            args.view_list_config
        )


class CreateOrReplaceConfigTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__(
            "create-or-replace-config-tables",
            "Copy local config tables (CSV) to BigQuery",
        )

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_config_tables_base_dir_file_argument(parser)
        add_table_names_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        table_names = args.table_names or get_local_config_table_names(
            args.config_tables_base_dir
        )
        update_or_create_config_tables(
            client, args.config_tables_base_dir, table_names, dataset=args.dataset
        )


class DeleteConfigTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__("delete-config-tables", "Delete Config Tables")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_config_tables_base_dir_file_argument(parser)
        add_table_names_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        table_names = args.table_names or get_local_config_table_names(
            args.config_tables_base_dir
        )
        table_names_dict = create_simple_view_mapping_from_view_list(args.dataset, table_names)
        delete_views_or_tables(client, table_names_dict)


SUB_COMMANDS: List[SubCommand] = [
    CreateOrReplaceViewsSubCommand(),
    DeleteViewsSubCommand(),
    MaterializeViewsSubCommand(),
    DeleteMaterializedTablesSubCommand(),
    DiffViewsSubCommand(),
    GetViewsSubCommand(),
    SortViewListSubCommand(),
    CreateOrReplaceConfigTablesSubCommand(),
    DeleteConfigTablesSubCommand(),
]

SUB_COMMAND_BY_NAME: Dict[str, SubCommand] = {
    sub_command.name: sub_command for sub_command in SUB_COMMANDS
}


def add_common_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--dataset", type=str, required=True, help="GCP BigQuery dataset"
    )

    parser.add_argument("--debug", action="store_true", help="Enable debug logging")


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=("BigQuery Views Manager"))

    subparsers = parser.add_subparsers(dest="command")
    subparsers.required = True
    for sub_command in SUB_COMMANDS:
        sub_parser = subparsers.add_parser(
            sub_command.name, help=sub_command.description
        )
        add_common_arguments(sub_parser)
        sub_command.add_arguments(sub_parser)

    return parser.parse_args(argv)


def run(args: argparse.Namespace):
    sub_command = SUB_COMMAND_BY_NAME[args.command]
    client = bigquery.Client()
    sub_command.run(client, args)


def main(argv=None):
    args = parse_args(argv)

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    LOGGER.debug("args: %s", args)

    run(args)


if __name__ == "__main__":
    logging.basicConfig(level="INFO")

    main()
