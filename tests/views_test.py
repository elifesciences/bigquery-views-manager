from bigquery_views_manager.views import get_view_template_file


class TestGetViewTemplateFile:
    def test_should_join_base_dir_and_view_name_with_sql_ext(self):
        assert str(get_view_template_file("views", "view1")) == "views/view1.sql"
