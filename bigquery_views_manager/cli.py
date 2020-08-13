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
    load_view_mapping,
    save_view_mapping,
    determine_view_insert_order,
    create_simple_view_mapping_from_view_list,
)

from .update_views import update_or_create_views
from .materialize_views import materialize_views
from .diff_views import diff_views
from .get_views import get_views
from .delete_views_or_tables import delete_views_or_tables
from .config_tables import get_local_config_table_names, update_or_create_config_tables

from . import configure_warnings  # noqa pylint: disable=unused-import


LOGGER = logging.getLogger(__name__)

DEFAULT_VIEW_LIST_FILE = "views/views.lst"
DEFAULT_MATERIALIZED_VIEW_LIST_FILE = "views/materialized-views.lst"

DEFAULT_CONFIG_TABLES_BASE_DIR = "config-tables"


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
        add_view_list_file_argument(parser)
        add_view_names_argument(parser)
        parser.add_argument(
            "--materialize",
            action="store_true",
            help="Materialize views in the materialized view list while updating",
        )
        add_materialized_view_list_file_argument(parser)
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        # structure of dict-
        # {TEMPLATE_FILE_NAME : {'dataset_name' : DATASET_NAME, 'table_name' : TABLE_NAME}}
        materialized_view_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )
        views_ordered_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )
        views_dict = (
            extend_or_subset_mapped_view_subset(
                views_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else views_ordered_dict_all
        )

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
            Path(args.view_list_file).parent,
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
        add_view_list_file_argument(parser)
        add_view_names_argument(parser)
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        views_ordered_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )

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
        add_view_list_file_argument(parser)
        add_materialized_view_list_file_argument(parser)
        add_view_names_argument(parser)
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        materialized_view_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )
        materialized_view_ordered_dict = (
            get_mapped_materialized_view_subset(
                materialized_view_ordered_dict_all, args.view_names
            )
            if args.view_names
            else materialized_view_ordered_dict_all
        )
        views_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )
        materialize_views(
            client,
            materialized_view_dict=materialized_view_ordered_dict,
            source_view_dict=views_dict_all,
            project=client.project,
        )


class DeleteMaterializedTablesSubCommand(SubCommand):
    def __init__(self):
        super().__init__("delete-materialized-tables", "Delete Materialized Tables")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_materialized_view_list_file_argument(parser)
        add_view_names_argument(parser)
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        materialized_views_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )

        materialized_views_ordered_dict = (
            extend_or_subset_mapped_view_subset(
                materialized_views_ordered_dict_all, args.view_names, args.dataset
            )
            if args.view_names
            else materialized_views_ordered_dict_all
        )
        delete_views_or_tables(client, materialized_views_ordered_dict)


class DiffViewsSubCommand(SubCommand):
    def __init__(self):
        super().__init__("diff-views", "Show difference between local and remote views")

    def add_arguments(self, parser: argparse.ArgumentParser):
        add_view_list_file_argument(parser)
        add_materialized_view_list_file_argument(parser)
        add_view_names_argument(parser)
        parser.add_argument(
            "--fail-if-changed", action="store_true", help="Fail if changed"
        )
        disable_view_name_mapping_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        materialized_view_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )
        views_ordered_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )
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
            Path(args.view_list_file).parent,
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
        add_view_list_file_argument(parser)
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
        add_materialized_view_list_file_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        to_map_table_name = not args.disable_view_name_mapping
        views_ordered_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )
        materialized_view_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )
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

        base_dir = Path(args.view_list_file).parent
        get_views(client, base_dir, views_dict, project=client.project)
        if args.all_remote_views:
            for view_template_name, dataset_table_or_view_data in views_dict.items():
                views_ordered_dict_all[view_template_name] = dataset_table_or_view_data

        if args.add_to_view_list:
            merged_view_names = determine_view_insert_order(
                base_dir, views_ordered_dict_all, materialized_view_ordered_dict_all
            )
            save_view_mapping(
                args.view_list_file, merged_view_names, is_materialized_view=False
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
        add_view_list_file_argument(parser)
        disable_view_name_mapping_argument(parser)
        add_materialized_view_list_file_argument(parser)

    def run(self, client: bigquery.Client, args: argparse.Namespace):
        base_dir = Path(args.view_list_file).parent
        to_map_table_name = not args.disable_view_name_mapping
        views_ordered_dict_all = load_view_mapping(
            args.view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
        )

        materialized_view_ordered_dict_all = load_view_mapping(
            args.materialized_view_list_file,
            should_map_table=to_map_table_name,
            default_dataset_name=args.dataset,
            is_materialized_view=True,
        )

        sorted_view_names = determine_view_insert_order(
            base_dir, views_ordered_dict_all, materialized_view_ordered_dict_all
        )

        sorted_view_dict = create_simple_view_mapping_from_view_list(
            args.dataset, sorted_view_names
        )
        save_view_mapping(args.view_list_file, sorted_view_dict, False)


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
