from collections import OrderedDict

from typing import List, Tuple

from bigquery_views_manager.view_list import (
    get_referenced_table_names_for_query,
    determine_insert_order_for_view_names_and_referenced_tables,
    DATASET_NAME_KEY,
    VIEW_OR_TABLE_NAME_KEY,
)

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
