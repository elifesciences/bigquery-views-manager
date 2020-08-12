import logging
import re
from pathlib import Path
from typing import Dict, List, Set
from collections import OrderedDict

from .views import get_local_view_template

LOGGER = logging.getLogger(__name__)

TEMPLATE_TABLE_PREFIX = "{project}.{dataset}."
DATASET_NAME_KEY = "dataset_name"
VIEW_OR_TABLE_NAME_KEY = "table_name"


def get_default_destination_table_name_for_view_name(view_name: str) -> str:
    return "m" + view_name


def get_mapped_materialized_view_subset(
        materialized_view_ordered_dict_all: OrderedDict,
        subset_view_template_names: Set[str],
):
    materialized_view_ordered_dict = OrderedDict()
    for (
            template_file_name,
            dataset_view_or_table_data,
    ) in materialized_view_ordered_dict_all.items():
        if template_file_name in subset_view_template_names:
            materialized_view_ordered_dict.update(
                {template_file_name: dataset_view_or_table_data})
    return materialized_view_ordered_dict


def map_view_to_dataset_from_template_mapping_dict(
        template_mapping_dict: OrderedDict):
    return {
        view.get(VIEW_OR_TABLE_NAME_KEY): view.get(DATASET_NAME_KEY)
        for view in list(template_mapping_dict.values())
    }


def extend_or_subset_mapped_view_subset(
        views_ordered_dict_all,
        view_names_for_subset_extend: List[str],
        default_dataset: str,
):
    views_dict = OrderedDict()
    for view_name in view_names_for_subset_extend:
        views_dict[view_name] = views_ordered_dict_all.get(
            view_name,
            {
                DATASET_NAME_KEY: default_dataset,
                VIEW_OR_TABLE_NAME_KEY: view_name
            },
        )
    return views_dict


def load_view_mapping(
        filename: str,
        should_map_table: bool,
        default_dataset_name: str,
        is_materialized_view: bool = False,
) -> OrderedDict:
    view_mapping = OrderedDict()
    view_mapping_as_string = Path(filename).read_text().splitlines()
    for line in view_mapping_as_string:
        if line.strip() == "":
            continue
        splitted_line = line.split(",")
        view_template_file_name = splitted_line[0]
        if len(splitted_line) < 2 or not should_map_table:
            dataset_name = default_dataset_name
            table_name = (get_default_destination_table_name_for_view_name(
                view_template_file_name)
                          if is_materialized_view else view_template_file_name)
        else:
            if is_materialized_view:
                mapped_full_name = splitted_line[1].split(".")
                dataset_name = (default_dataset_name
                                if len(mapped_full_name) < 2 else
                                mapped_full_name[0].strip())
                table_name = (mapped_full_name[0] if len(mapped_full_name) < 2
                              else mapped_full_name[1])
            else:
                table_name = view_template_file_name
                dataset_name = splitted_line[1]

        view_mapping[view_template_file_name] = {
            DATASET_NAME_KEY: dataset_name,
            VIEW_OR_TABLE_NAME_KEY: table_name,
        }
    return view_mapping


def create_simple_view_mapping_from_view_list(dataset: str,
                                              view_name_list: List[str]):
    view_mapping = OrderedDict()
    for view_name in view_name_list:
        view_mapping.update({
            view_name: {
                VIEW_OR_TABLE_NAME_KEY: view_name,
                DATASET_NAME_KEY: dataset
            }
        })
    return view_mapping


def save_view_mapping(filename: str, view_mapping: OrderedDict,
                      is_materialized_view: False):
    LOGGER.info("saving view mapping list to %s", filename)
    file_content_as_list = list()
    for view_template_name, view_dict in view_mapping.items():
        if is_materialized_view:
            file_content_as_list.append(
                view_template_name + "," + view_dict.get(DATASET_NAME_KEY) +
                ".",
                view_dict.get(VIEW_OR_TABLE_NAME_KEY),
            )
        else:
            file_content_as_list.append(view_template_name + "," +
                                        view_dict.get(DATASET_NAME_KEY))

    file_content = "\n".join(file_content_as_list) + "\n"
    return Path(filename).write_text(file_content)


def get_referenced_table_names_for_query(view_query: str) -> List[str]:
    return re.findall(r"`(.*)`", view_query)


def get_referenced_table_names_for_view_name(base_dir: str,
                                             view_name: str) -> List[str]:
    return get_referenced_table_names_for_query(
        get_local_view_template(base_dir, view_name).view_template_content)


def get_referenced_table_names_by_view_name_map(base_dir: str,
                                                view_names: List[str]
                                                ) -> Dict[str, List[str]]:
    return {
        view_name:
        get_referenced_table_names_for_view_name(base_dir, view_name)
        for view_name in view_names
    }


def get_short_table_name(table_name: str) -> str:
    if table_name.startswith(TEMPLATE_TABLE_PREFIX):
        return table_name[len(TEMPLATE_TABLE_PREFIX):]
    return table_name


def get_resolved_short_table_name(
        table_name: str,
        view_by_materialized_view_name_map: Dict[str, str]) -> str:
    short_table_name = get_short_table_name(table_name)
    return view_by_materialized_view_name_map.get(short_table_name,
                                                  short_table_name)


def filter_map_values_in(unfiltered_map: Dict[str, List[str]],
                         include_list: List[str]) -> Dict[str, List[str]]:
    return {
        k: [v for v in values if v in include_list]
        for k, values in unfiltered_map.items()
    }


def add_names_with_referenced_names_recursively(
        result_name_list: List[str],
        name_list: List[str],
        referenced_names_by_name_map: Dict[str, List[str]],
) -> List[str]:
    for name in name_list:
        add_names_with_referenced_names_recursively(
            result_name_list,
            referenced_names_by_name_map.get(name, []),
            referenced_names_by_name_map,
        )
        if name not in result_name_list:
            result_name_list.append(name)
    return result_name_list


def determine_insert_order_for_view_names_and_referenced_tables(
        view_mapping: OrderedDict,
        referenced_table_names_by_view_name: Dict[str, List[str]],
        materialized_views_ordered_dict: OrderedDict,
) -> OrderedDict:
    view_by_materialized_view_name_map = {
        dataset_view_data.get(VIEW_OR_TABLE_NAME_KEY): template_name
        for template_name, dataset_view_data in
        materialized_views_ordered_dict.items()
    }
    short_referenced_table_names_by_view_name = filter_map_values_in(
        {
            view_name: [
                get_resolved_short_table_name(
                    referenced_table_name, view_by_materialized_view_name_map)
                for referenced_table_name in referenced_table_names
            ]
            for view_name, referenced_table_names in
            referenced_table_names_by_view_name.items()
        },
        view_mapping,
    )
    all_view_names = list(view_mapping.keys())
    result_view_names = []
    result_view_names = add_names_with_referenced_names_recursively(
        result_view_names, all_view_names,
        short_referenced_table_names_by_view_name)

    view_insert_order_ordereddict = OrderedDict()
    for result_view_name in result_view_names:
        view_insert_order_ordereddict[result_view_name] = view_mapping.get(result_view_name)

    return view_insert_order_ordereddict


def determine_view_insert_order(
        base_dir: str,
        view_names_ordered_dict: OrderedDict,
        materialized_views_ordered_dict: OrderedDict,
) -> OrderedDict:
    return determine_insert_order_for_view_names_and_referenced_tables(
        view_names_ordered_dict,
        get_referenced_table_names_by_view_name_map(base_dir,
                                                    view_names_ordered_dict),
        materialized_views_ordered_dict,
    )
