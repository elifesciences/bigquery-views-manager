from pathlib import Path
import re


def replace_query_with_placeholders(query: str, project: str) -> str:
    regex_pattern = "".join(["`", project, r"\.[\w]*\."])
    n_query = re.sub(regex_pattern, "`{project}.{dataset}.", query)
    return n_query


def resolve_query_template_placeholders(
        query_template: str,
        project: str,
        default_dataset: str,
        view_to_dataset_mapping: dict,
) -> str:
    dataset_placeholder = "{dataset}."
    project_placeholder = "{project}."
    placeholder = "".join([project_placeholder, dataset_placeholder])
    regex_pattern = "".join(
        [project_placeholder, dataset_placeholder, r"[\w]*"])
    all_occurrences = re.findall(regex_pattern, query_template)
    tables_present_in_template = [
        x.replace(placeholder, "") for x in all_occurrences
    ]

    for table in tables_present_in_template:
        dataset_table_placeholder_to_replace = "".join(
            [dataset_placeholder, table])
        new_dataset_table_name = "".join(
            [view_to_dataset_mapping.get(table, default_dataset), ".", table])
        query_template = (query_template.replace("{project}", project).replace(
            dataset_table_placeholder_to_replace,
            new_dataset_table_name).replace("{dataset}", default_dataset))
    return query_template


def normalize_view_template(query_template: str) -> str:
    return query_template.rstrip() + '\n'


class ViewTemplate:
    def __init__(self, view_template_content):
        self.view_template_content = view_template_content

    @staticmethod
    def from_file(filename: str) -> "ViewTemplate":
        return ViewTemplate(Path(filename).read_text())

    @staticmethod
    def from_query(query: str, project: str) -> "ViewTemplate":
        return ViewTemplate(
            replace_query_with_placeholders(query, project=project)
        )

    @property
    def normalized(self) -> "ViewTemplate":
        return ViewTemplate(normalize_view_template(
            self.view_template_content
        ))

    def __repr__(self):
        return "%s(%r)" % (type(self).__name__, self.view_template_content)

    def __str__(self):
        return self.view_template_content

    def to_file(self, filename: str):
        return Path(filename).write_text(self.view_template_content)

    def substitute(self, project: str, default_dataset: str,
                   view_to_dataset_mapping: dict) -> str:
        return resolve_query_template_placeholders(
            self.view_template_content,
            project=project,
            default_dataset=default_dataset,
            view_to_dataset_mapping=view_to_dataset_mapping,
        )
