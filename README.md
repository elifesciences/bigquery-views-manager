# BigQuery Views Manager

[![PyPi version](https://pypip.in/v/bigquery-views-manager/badge.png)](https://pypi.org/project/bigquery-views-manager/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Utility project to maintain BigQuery views. The main interface is the CLI.

Main features:

* Synchronize BigQuery Views between GCP and local file system
* Materialize Views (by running a view and saving it to a table):
  * [BigQuery Materialized Views](https://cloud.google.com/bigquery/docs/materialized-views-intro) are now available as Pre-GA

## Pre-requisites

* Python 3
* [Google Cloud SDK](https://cloud.google.com/sdk/docs/) for [gcloud](https://cloud.google.com/sdk/gcloud/)

## Install

```bash
pip install bigquery-views-manager
```

## Configuration

### Views SQL files

SQL code of the view queries is assumed to be in files with the `.sql` files. By default they will be in the `views` directory.

The files can contain placeholders (surrounded by curly brackets, e.g. `{placeholder}`) for the following variables:

| name | description |
| ---- | ----------- |
| project | The GCP project |
| dataset | The BigQuery dataset |

Using the placeholders allows you to deploy the views to for example test, staging and production separately.

Example:

```sql
SELECT *
FROM `{project}.{dataset}.view1`
```

### View List Config `views.yml`

The `views.yml` file contains the list of views that should be processed. It is important that the list of views are in the correct insert order. i.e. if `v_view2` depends on `v_view1` then `v_view1` should appear first.

The format is a yaml file. In the simplest case it will be the list of the views, e.g.:

```yaml
- v_view1
- v_view2
```

Additional parameters can be added, e.g. to materialize `v_view1`:

```yaml
- v_view1:
    materialize: true
- v_view2
```

Or to materialize `v_view1` to another table name:

```yaml
- v_view1:
    materialize: true
    materialize_as: output_table1
- v_view2
```

The dataset could also be specified:

```yaml
- v_view1:
    materialize: true
    materialize_as: output_dataset1.output_table1
- v_view2
```

When working with multiple datasets, this can also be conditional:

```yaml
- v_view1:
    materialize: true
    conditions:
    - if:
        dataset: source_dataset1
      materialize_as: "output_dataset1.output_table1"
- v_view2
```

The condition will depend on the passed in `--dataset`.

### Config Tables

Config tables are tables loaded from CSV. They are meant to assist views with configuration data, rather than loading large data. Config tables are generally used by views to avoid having to hard-code certain values in the views.

It is assumed that the filename is target table name with the `.csv` file extension. By default in the `./config-tables/tables` directory. A BigQuery table schema can be specified via a file with the `_schema.json` in the `./config-tables/schema` directory.

Another directory can be specified via the `--config-tables-base-dir` CLI argument.

### Example Data

See [example-data](https://github.com/elifesciences/bigquery-views-manager/tree/develop/example-data).

## BigQuery Views Manager CLI

To get the command help:

```bash
python -m bigquery_views_manager --help
```

Or:

```bash
python -m bigquery_views_manager <sub-command> --help
```

### Create or Replace Views

```bash
python -m bigquery_views_manager \
    create-or-replace-views \
    --dataset=my_dataset \
    [--view-list-config=/path/to/views.yml] \
    [<view name> [<other view name> ...]]
```

Adding the `--materialize` flag will additionally materialize the views (where it has been enabled). In that case views will be materialized immediately after updating a view.

### Materialize Views

```bash
python -m bigquery_views_manager \
    materialize-views \
    --dataset=my_dataset \
    [--view-list-config=/path/to/views.yml] \
    [<view name> [<other view name> ...]]
```

### Diff Views

Show differences between local views and views within BigQuery.

```bash
python -m bigquery_views_manager \
    diff-views \
    --dataset=my_dataset \
    [--view-list-config=/path/to/views.yml] \
    [<view name> [<other view name> ...]]
```

### Get Views

Copy views from BigQuery to the local file system.

To get all of the files listed in `views/views.yml`:

```bash
python -m bigquery_views_manager \
    get-views \
    --dataset=my_dataset \
    [--view-list-config=/path/to/views.yml]
```

To get a particular view or views:

```bash
python -m bigquery_views_manager \
    get-views \
    --dataset=my_dataset \
    [--view-list-config=/path/to/views.yml] \
    <view name> [<other view name> ...]
```

When views are retrieved, the project name and dataset are replaced with placeholders.

### Update Config Tables

Copy config tables (CSV) to BigQuery. The config tables are by default stored in `./config-tables`.

```bash
python -m bigquery_views_manager \
    create-or-replace-config-tables \
    --dataset=my_dataset \
    [--config-tables-base-dir=/path/to/config-tables] \
    [<table name> ...]
```

### Adding a View

Add the view to the `views` directory with the view name and `.sql` file extension.

The view name also needs to be added to `views/views.yml` in the correct order (i.e. if a view depends on another view, the other view should appear first).

### Cleanup Sub Commands

The CLI also supports additional sub commands to delete views etc. Those are in particular use-ful in a CI environment.

The following commands are supported:

* `delete-config-tables`
* `delete-views`
* `delete-materialized-tables`

## Related Projects

* [BigQuery-DatasetManager](https://github.com/laughingman7743/BigQuery-DatasetManager)
* [BigQuery View Analyzer](https://github.com/servian/bigquery-view-analyzer)
