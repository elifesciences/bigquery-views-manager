import logging
import re
from pathlib import Path
from typing import Dict, List, Set, Union
from collections import OrderedDict

import yaml

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
    LOGGER.debug('referenced_table_names_by_view_name: %s', referenced_table_names_by_view_name)
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


class ViewCondition:
    def __init__(
            self,
            if_condition: Dict[str, str],
            materialize_as: str = None):
        self.if_condition = if_condition
        self.materialize_as = materialize_as

    @staticmethod
    def from_value(value: dict) -> 'ViewCondition':
        return ViewCondition(
            if_condition=value.get('if'),
            materialize_as=value.get('materialize_as')
        )

    def to_value(self) -> dict:
        value = {}
        if self.if_condition is not None:
            value['if'] = self.if_condition
        if self.materialize_as is not None:
            value['materialize_as'] = self.materialize_as
        return value

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return '%s(if_condition=%r, materialize_as=%r)' % (
            type(self).__name__,
            self.if_condition,
            self.materialize_as
        )

    def get_values(self) -> dict:
        return {
            key: value
            for key, value in self.__dict__.items()
            if key != 'if_condition' and value is not None
        }

    def is_matching(self, condition_values: dict) -> bool:
        for key, value in self.if_condition.items():
            if condition_values.get(key) != value:
                return False
        return True


class ViewConfig:
    def __init__(
            self,
            view_name: str,
            materialize: bool = None,
            materialize_as: str = None,
            conditions: List[ViewCondition] = None):
        self.view_name = view_name
        self.materialize = materialize
        self.materialize_as = materialize_as
        self.conditions = conditions or []

    @staticmethod
    def from_value(value: Union[str, dict]) -> 'ViewConfig':
        if isinstance(value, str):
            return ViewConfig(value)
        if len(value) == 1:
            view_name, view_args = next(iter(value.items()))
            conditions = [
                ViewCondition.from_value(condition)
                for condition in view_args.get('conditions', [])
            ]
            return ViewConfig(
                view_name,
                materialize=view_args.get('materialize'),
                conditions=conditions
            )
        raise ValueError('unrecognised view config: %r' % value)

    def to_value(self) -> Union[str, dict]:
        view_args = {}
        if self.materialize is not None:
            view_args['materialize'] = self.materialize
        if self.materialize_as is not None:
            view_args['materialize_as'] = self.materialize_as
        if self.conditions:
            view_args['conditions'] = [
                condition.to_value()
                for condition in self.conditions
            ]
        if not view_args:
            return str(self)
        return {self.view_name: view_args}

    def __str__(self):
        return self.view_name

    def __repr__(self):
        return '%s(%r, materialize=%r, materialize_as=%r, conditions=%r)' % (
            type(self).__name__,
            self.view_name,
            self.materialize,
            self.materialize_as,
            self.conditions
        )

    @property
    def resolved_materialize_as(self):
        if self.materialize_as:
            return self.materialize_as
        if self.materialize:
            return get_default_destination_table_name_for_view_name(
                self.view_name
            )
        return None

    def apply_conditional_values(self, condition: ViewCondition) -> 'ViewConfig':
        return ViewConfig(**{
            **self.__dict__,
            **condition.get_values()
        })

    def resolve_conditions(self, condition_value: dict) -> 'ViewConfig':
        for condition in self.conditions:
            if not condition.is_matching(condition_value):
                continue
            return self.apply_conditional_values(condition)
        return self


class ViewListConfig:
    def __init__(self, view_config_list: List[ViewConfig]):
        self.view_config_list = view_config_list

    def __str__(self):
        return str(self.view_config_list)

    def __repr__(self):
        return '%s(%r)' % (
            type(self).__name__,
            self.view_config_list
        )

    def __len__(self):
        return len(self.view_config_list)

    def __iter__(self):
        return iter(self.view_config_list)

    def __getitem__(self, index):
        return self.view_config_list[index]

    @property
    def view_names(self) -> List[str]:
        return [view.view_name for view in self.view_config_list]

    def filter_view_names(self, view_names: List[str]) -> 'ViewListConfig':
        return ViewListConfig([
            view
            for view in self.view_config_list
            if view.view_name in view_names
        ])

    def resolve_conditions(self, condition_value: dict) -> 'ViewListConfig':
        return ViewListConfig([
            view.resolve_conditions(condition_value)
            for view in self.view_config_list
        ])

    def has_view(self, view_name: str) -> bool:
        return any(view.view_name == view_name for view in self.view_config_list)

    def add_view(self, view: ViewConfig) -> 'ViewListConfig':
        return ViewListConfig(self.view_config_list + [view])

    def sort_insert_order(self, base_dir: str) -> 'ViewListConfig':
        dummy_dataset = 'dummy_dataset'
        insert_order = determine_view_insert_order(
            base_dir,
            view_names_ordered_dict=self.to_views_ordered_dict(dummy_dataset),
            materialized_views_ordered_dict=self.to_materialized_view_ordered_dict(dummy_dataset)
        )
        view_config_by_name_map = {
            view.view_name: view
            for view in self.view_config_list
        }
        LOGGER.debug('insert_order: %s', insert_order)
        return ViewListConfig([
            view_config_by_name_map[view_name]
            for view_name in insert_order.keys()
        ])

    def to_views_ordered_dict(self, dataset: str) -> OrderedDict:
        return OrderedDict([
            (
                view.view_name,
                {
                    DATASET_NAME_KEY: dataset,
                    VIEW_OR_TABLE_NAME_KEY: view.view_name
                }
            )
            for view in self.view_config_list
        ])

    def to_materialized_view_ordered_dict(self, dataset: str) -> OrderedDict:
        result = OrderedDict()
        for view in self.view_config_list:
            resolved_materialize_as = view.resolved_materialize_as
            if not resolved_materialize_as:
                continue
            full_name_parts = resolved_materialize_as.split('.', maxsplit=1)
            if len(full_name_parts) == 1:
                full_name_parts = (dataset, full_name_parts[0])
            output_dataset_name, output_table_name = full_name_parts
            result[view.view_name] = {
                DATASET_NAME_KEY: output_dataset_name,
                VIEW_OR_TABLE_NAME_KEY: output_table_name
            }
        return result


def load_view_list_config(path: str):
    view_list_obj = yaml.safe_load(Path(path).read_text())
    LOGGER.debug('view_list_obj: %s', view_list_obj)
    return ViewListConfig([
        ViewConfig.from_value(value)
        for value in view_list_obj
    ])


def save_view_list_config(view_list_config: ViewListConfig, path: str):
    Path(path).write_text(yaml.safe_dump([
        view.to_value()
        for view in view_list_config
    ]))
