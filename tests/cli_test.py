from collections import OrderedDict

from bigquery_views_manager.cli import load_view_mapping
from bigquery_views_manager.view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

VIEW_1 = "view1,dataset1"
VIEW_2 = "view2,dataset2"


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
