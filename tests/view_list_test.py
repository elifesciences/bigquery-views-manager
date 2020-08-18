import logging
from pathlib import Path
from collections import OrderedDict

from typing import List, Tuple

from bigquery_views_manager.view_list import (
    get_referenced_table_names_for_query,
    determine_insert_order_for_view_names_and_referenced_tables,
    DATASET_NAME_KEY,
    VIEW_OR_TABLE_NAME_KEY,
    load_view_list_config
)


LOGGER = logging.getLogger(__name__)


VIEW_1 = "view1"
VIEW_2 = "view2"
VIEW_3 = "view3"
M_VIEW_2 = "materialized_view2"
TABLE_NAME = "table1"

DATASET_1 = "dataset"


def get_input_ordered_dict_view_mapping(
        view_dataset_mapping: List[Tuple[str, str, str]]):
    view_mapping = OrderedDict()
    for dataset, view_template_name, db_view_name in view_dataset_mapping:
        view_mapping[view_template_name] = {
            DATASET_NAME_KEY: dataset,
            VIEW_OR_TABLE_NAME_KEY: db_view_name,
        }
    return view_mapping


def get_referenced_table_in_template(
        ref_table_as_simple_list: List[Tuple[str, List[str]]],
        compose_full_table_name_with_placeholder: bool,
):
    ref_table = OrderedDict()
    for view_template_name, ref_table_list in ref_table_as_simple_list:
        ref_table[view_template_name] = ([
            "".join(["{project}.{dataset}.", ref_table])
            for ref_table in ref_table_list
        ] if compose_full_table_name_with_placeholder else ref_table_list)
    return ref_table


class TestGetReferencedTableNamesForQuery:
    def test_should_find_single_table_reference(self):
        assert get_referenced_table_names_for_query("""
            SELECT * FROM `{project}.{dataset}.table1`
            """) == ["{project}.{dataset}.table1"]


class TestDetermineInsertOrderForViewNamesAndReferencedTables:
    def test_should_find_insert_order_for_single_view(self):
        result = OrderedDict()
        result[VIEW_1] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_1}
        assert determine_insert_order_for_view_names_and_referenced_tables(
            view_mapping=get_input_ordered_dict_view_mapping([
                (DATASET_1, VIEW_1, VIEW_1)
            ]),
            referenced_table_names_by_view_name=get_referenced_table_in_template(
                [(VIEW_1, [TABLE_NAME])],
                compose_full_table_name_with_placeholder=False),
            materialized_views_ordered_dict=OrderedDict(),
        ) == result

    def test_should_find_insert_order_for_view_depending_on_another_view(self):
        result = OrderedDict()
        result[VIEW_2] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_2}
        result[VIEW_1] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_1}
        assert determine_insert_order_for_view_names_and_referenced_tables(
            view_mapping=get_input_ordered_dict_view_mapping([
                (DATASET_1, VIEW_1, VIEW_1), (DATASET_1, VIEW_2, VIEW_2)
            ]),
            referenced_table_names_by_view_name=get_referenced_table_in_template(
                [(VIEW_1, [TABLE_NAME, VIEW_2])],
                compose_full_table_name_with_placeholder=False,
            ),
            materialized_views_ordered_dict=OrderedDict(),
        ) == result

    def test_should_find_insert_order_for_view_depending_on_another_view_with_placeholders(
            self):
        result = OrderedDict()
        result[VIEW_2] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_2}
        result[VIEW_1] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_1}
        assert determine_insert_order_for_view_names_and_referenced_tables(
            view_mapping=get_input_ordered_dict_view_mapping([
                (DATASET_1, VIEW_1, VIEW_1), (DATASET_1, VIEW_2, VIEW_2)
            ]),
            referenced_table_names_by_view_name=get_referenced_table_in_template(
                [(VIEW_1, [TABLE_NAME, VIEW_2])],
                compose_full_table_name_with_placeholder=True,
            ),
            materialized_views_ordered_dict=OrderedDict(),
        ) == result

    def test_should_find_insert_order_for_view_depending_on_materialized_view_with_placeholders(
            self):
        result = OrderedDict()
        result[VIEW_2] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_2}
        result[VIEW_1] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_1}
        assert determine_insert_order_for_view_names_and_referenced_tables(
            view_mapping=get_input_ordered_dict_view_mapping([
                (DATASET_1, VIEW_1, VIEW_1), (DATASET_1, VIEW_2, VIEW_2)
            ]),
            referenced_table_names_by_view_name=get_referenced_table_in_template(
                [(VIEW_1, [TABLE_NAME, M_VIEW_2])],
                compose_full_table_name_with_placeholder=True,
            ),
            materialized_views_ordered_dict=get_input_ordered_dict_view_mapping(
                [(DATASET_1, VIEW_2, M_VIEW_2)]),
        ) == result

    def test_should_find_insert_order_for_indirect_dependency(self):
        result = OrderedDict()
        result[VIEW_3] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_3}
        result[VIEW_2] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_2}
        result[VIEW_1] = {DATASET_NAME_KEY: DATASET_1, VIEW_OR_TABLE_NAME_KEY: VIEW_1}
        assert determine_insert_order_for_view_names_and_referenced_tables(
            view_mapping=get_input_ordered_dict_view_mapping([
                (DATASET_1, VIEW_1, VIEW_1),
                (DATASET_1, VIEW_2, VIEW_2),
                (DATASET_1, VIEW_3, VIEW_3),
            ]),
            referenced_table_names_by_view_name=get_referenced_table_in_template(
                [(VIEW_1, [TABLE_NAME, VIEW_2]),
                 (VIEW_2, [TABLE_NAME, VIEW_3])],
                compose_full_table_name_with_placeholder=False,
            ),
            materialized_views_ordered_dict=OrderedDict(),
        ) == result


class TestLoadViewListConfig:
    def test_should_load_simple_yaml_with_defaults(self, temp_dir: Path):
        view_list_path = temp_dir / 'views.yaml'
        view_list_path.write_text('\n'.join([
            '- view1',
            '- view2'
        ]))
        view_list = load_view_list_config(view_list_path)
        LOGGER.debug('view_list: %s', view_list)
        assert len(view_list) == 2
        assert [str(item) for item in view_list] == ['view1', 'view2']

    def test_should_load_yaml_with_materialize_flag(self, temp_dir: Path):
        view_list_path = temp_dir / 'views.yaml'
        view_list_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true',
            '- view2:',
            '    materialize: false',
            '- view3'
        ]))
        view_list = load_view_list_config(view_list_path)
        LOGGER.debug('view_list: %s', view_list)
        assert len(view_list) == 3
        assert [str(item) for item in view_list] == ['view1', 'view2', 'view3']
        assert [item.materialize for item in view_list] == [True, False, None]
        assert [item.resolved_materialize_as for item in view_list] == [
            'mview1',
            None,
            None
        ]

    def test_should_load_yaml_with_materialize_to_config(self, temp_dir: Path):
        view_list_path = temp_dir / 'views.yaml'
        view_list_path.write_text('\n'.join([
            '- view1:',
            '    materialize: true',
            '    conditions:',
            '    - if:',
            '        dataset: source_dataset1',
            '      materialize_as: "output_dataset1.output_table1"'
        ]))
        view_list = load_view_list_config(view_list_path)
        LOGGER.debug('view_list: %s', view_list)
        assert len(view_list) == 1
        assert [str(item) for item in view_list] == ['view1']
        view1 = view_list[0]
        assert view1.materialize
        assert view1.materialize_as is None
        assert len(view1.conditions) == 1
        assert view1.conditions[0].if_condition == {
            'dataset': 'source_dataset1'
        }
        assert view1.conditions[0].materialize_as == 'output_dataset1.output_table1'

        matching_resolved_view1 = view1.resolve_conditions({'dataset': 'source_dataset1'})
        assert matching_resolved_view1.materialize_as == 'output_dataset1.output_table1'
        assert matching_resolved_view1.resolved_materialize_as == 'output_dataset1.output_table1'

        not_matching_resolved_view1 = view1.resolve_conditions({'dataset': 'other'})
        assert not_matching_resolved_view1.materialize_as is None
        assert not_matching_resolved_view1.resolved_materialize_as == 'mview1'
