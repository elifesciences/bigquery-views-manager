from bigquery_views_manager.view_template import ViewTemplate

TEMPLATE_1 = "SELECT * FROM `{project}.{dataset}.table1"

DATASET = "dataset1"
VIEW_1 = "table1"
VIEW_2 = "table2"
VIEW_TO_DATASET_MAPPING = {VIEW_1: DATASET, VIEW_2: DATASET}


class TestViewTemplate:
    def test_should_replace_multiple_project_and_dataset_references(self):
        view_template = ViewTemplate("""
            SELECT * FROM `{project}.{dataset}.table1`
            JOIN `{project}.{dataset}.table2`
            """)
        assert view_template.substitute(
            project="project1",
            default_dataset=DATASET,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        ) == ("""
            SELECT * FROM `project1.dataset1.table1`
            JOIN `project1.dataset1.table2`
            """)

    def test_should_ignore_unrelated_curly_brackets(self):
        view_template = ViewTemplate("""
            SELECT * FROM `{project}.{dataset}.table1`
            WHERE x = '{'
            """)
        assert view_template.substitute(
            project="project1",
            default_dataset=DATASET,
            view_to_dataset_mapping=VIEW_TO_DATASET_MAPPING,
        ) == ("""
            SELECT * FROM `project1.dataset1.table1`
            WHERE x = '{'
            """)

    def test_should_load_template_from_file(self, tmpdir):
        filename = tmpdir.join("view1.sql")
        filename.write(TEMPLATE_1)
        assert ViewTemplate.from_file(
            filename).view_template_content == TEMPLATE_1

    def test_should_load_template_from_query_with_matching_project(self):
        assert ViewTemplate.from_query(
            """
            SELECT * FROM `project1.dataset1.table1`
            JOIN `project1.dataset1.table2`
            """,
            project="project1",
        ).view_template_content == ("""
            SELECT * FROM `{project}.{dataset}.table1`
            JOIN `{project}.{dataset}.table2`
            """)

    def test_should_load_template_from_query_with_different_dataset(self):
        assert ViewTemplate.from_query(
            """
            SELECT * FROM `project1.dataset1.table1`
            JOIN `project1.dataset2.table2`
            """,
            project="project1",
        ).view_template_content == ("""
            SELECT * FROM `{project}.{dataset}.table1`
            JOIN `{project}.{dataset}.table2`
            """)

    def test_should_normalize_by_adding_line_feed(self):
        assert ViewTemplate.from_query(
            "SELECT * FROM `project1.dataset1.table1`",
            project="project1",
        ).normalized.view_template_content == (
            "SELECT * FROM `{project}.{dataset}.table1`\n"
        )
