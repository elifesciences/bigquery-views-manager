import logging
from difflib import context_diff
from typing import List, Set, Iterable
from collections import OrderedDict
import re

from google.cloud import bigquery

import crayons

from .update_views import get_local_view_query
from .views import get_bq_view_query, get_bq_view_names
from .view_list import DATASET_NAME_KEY, VIEW_OR_TABLE_NAME_KEY

LOGGER = logging.getLogger(__name__)

NONE_TEXT = "NONE"


class ChangedView:
    def __init__(
            self,
            dataset_name: str,
            view_name: str,
            local_view_query: str,
            remote_view_query: str,
    ):
        self.dataset_name = dataset_name
        self.view_name = view_name
        self.local_view_query = local_view_query
        self.remote_view_query = remote_view_query


class ViewDiffResult:
    def __init__(
            self,
            remote_only_view_names: Set[str],
            local_only_view_names: Set[str],
            unchanged_view_names: Set[str],
            changed_views: List[ChangedView],
    ):
        self.remote_only_view_names = remote_only_view_names
        self.local_only_view_names = local_only_view_names
        self.unchanged_view_names = unchanged_view_names
        self.changed_views = changed_views

    @property
    def changed_view_names(self):
        return {changed_view.view_name for changed_view in self.changed_views}


def get_dataset_to_table_dict(view_names: dict):
    dataset_to_table_dict = {}
    for _, value in view_names.items():
        dataset_name = value.get(DATASET_NAME_KEY)
        dataset_to_table_dict[dataset_name] = dataset_to_table_dict.get(
            dataset_name, [])
        dataset_to_table_dict[dataset_name].append(
            value.get(VIEW_OR_TABLE_NAME_KEY))
    return dataset_to_table_dict


def get_view_to_view_file(view_names: dict):
    view_to_view_file = dict()
    for k, value in view_names.items():
        view_to_view_file[value.get(DATASET_NAME_KEY) + "." +
                          value.get(VIEW_OR_TABLE_NAME_KEY)] = k
    return view_to_view_file


def get_diff_result(  # pylint: disable=too-many-arguments,too-many-locals
        client: bigquery.Client,
        base_dir: str,
        view_names_dict: OrderedDict,
        project: str,
        default_dataset: str,
        view_to_dataset_mapping: dict,
):
    dataset_to_table_list = get_dataset_to_table_dict(view_names_dict)
    view_to_view_file = get_view_to_view_file(view_names_dict)
    unchanged_view_names = set()
    local_view_names = [
        value.get(DATASET_NAME_KEY) + "." + value.get(VIEW_OR_TABLE_NAME_KEY)
        for k, value in view_names_dict.items()
    ]
    remote_view_names = []
    changed_views: List[ChangedView] = []

    for dataset, table_list in dataset_to_table_list.items():
        bq_view_names = get_bq_view_names(client, dataset=dataset)
        remote_view_names.extend([dataset + "." + x for x in bq_view_names])
        remote_and_local_views = set(bq_view_names) & set(table_list)
        for view_name in remote_and_local_views:
            view_file_name = view_to_view_file.get(dataset + "." + view_name)
            local_view_query = get_local_view_query(
                base_dir,
                view_file_name,
                project=project,
                default_dataset=default_dataset,
                view_to_dataset_mapping=view_to_dataset_mapping,
            )
            bq_view_query = get_bq_view_query(client,
                                              view_name,
                                              dataset=dataset)
            if re.sub(r"\s+", "",
                      local_view_query) == re.sub(r"\s+", "", bq_view_query):
                unchanged_view_names.add(dataset + "." + view_name)
            else:
                changed_views.append(
                    ChangedView(
                        dataset_name=dataset,
                        view_name=view_name,
                        local_view_query=local_view_query,
                        remote_view_query=bq_view_query,
                    ))
    return ViewDiffResult(
        remote_only_view_names=set(remote_view_names) - set(local_view_names),
        local_only_view_names=set(local_view_names) - set(remote_view_names),
        unchanged_view_names=unchanged_view_names,
        changed_views=changed_views,
    )


def format_list(value: Iterable[str]) -> str:
    return ", ".join(value) if value else NONE_TEXT


def highlight(value, is_good):
    return crayons.green(value) if is_good else crayons.red(value)


def format_diff_line(line):
    if line.startswith("!"):
        return crayons.red(line)
    if line.startswith("***") or line.startswith("---"):
        return crayons.white(line, bold=True)
    return line


def format_changed_view_diffs(changed_view: ChangedView) -> str:
    return "\n".join([
        str(crayons.red(line)) if line.startswith("!") else line
        for line in context_diff(
            changed_view.local_view_query.splitlines(),
            changed_view.remote_view_query.splitlines(),
            fromfile=f"{changed_view.view_name} (local)",
            tofile=f"{changed_view.view_name} (remote)",
        )
    ])


def format_diff_result(diff_result: ViewDiffResult) -> str:
    formatted_unchanged_view_names = highlight(format_list(
        diff_result.unchanged_view_names),
                                               is_good=True)
    formatted_remote_only_view_names = highlight(
        format_list(diff_result.remote_only_view_names),
        is_good=not diff_result.remote_only_view_names,
    )
    formatted_local_only_view_names = highlight(
        format_list(diff_result.local_only_view_names),
        is_good=not diff_result.local_only_view_names,
    )
    formatted_changed_view_names = highlight(
        format_list(diff_result.changed_view_names),
        is_good=not diff_result.changed_view_names,
    )
    if diff_result.changed_views:
        formatted_changed_view_diffs = (
            crayons.blue("changed views details BEGIN", bold=True) + "\n\n" +
            "\n\n".join([
                format_changed_view_diffs(changed_view)
                for changed_view in diff_result.changed_views
            ]) + "\n\n" +
            crayons.blue("changed views details END", bold=True) + "\n\n")
    else:
        formatted_changed_view_diffs = ""
    return formatted_changed_view_diffs + "\n".join([
        f"unchanged views : {formatted_unchanged_view_names}",
        f"remote only     : {formatted_remote_only_view_names}",
        f"local only      : {formatted_local_only_view_names}",
        f"changed views   : {formatted_changed_view_names}",
    ])


def diff_views(  # pylint: disable=too-many-arguments
        client: bigquery.Client,
        base_dir: str,
        view_names_dict: OrderedDict,
        project: str,
        default_dataset: str,
        view_to_dataset_mapping: dict,
):
    LOGGER.debug("view_names: %s", view_names_dict)
    diff_result = get_diff_result(
        client,
        base_dir,
        view_names_dict,
        project=project,
        default_dataset=default_dataset,
        view_to_dataset_mapping=view_to_dataset_mapping,
    )
    LOGGER.info("diff_result:\n%s", format_diff_result(diff_result))
    return diff_result.changed_views
