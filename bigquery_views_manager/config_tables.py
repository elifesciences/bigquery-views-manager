import logging
import json
from pathlib import Path
from typing import List

from google.cloud import bigquery
from google.cloud.bigquery.job import LoadJobConfig
from google.cloud.bigquery.schema import SchemaField

LOGGER = logging.getLogger(__name__)

CONFIG_TABLES_DIR = "tables"
CONFIG_TABLES_SCHEMA_DIR = "schema"


def get_local_config_table_names(base_dir: str) -> List[str]:
    return [
        p.stem
        for p in Path(base_dir).joinpath(CONFIG_TABLES_DIR).glob("*.csv")
    ]


def get_config_table_file(base_dir: str, config_table_name: str) -> str:
    return (Path(base_dir).joinpath(CONFIG_TABLES_DIR).joinpath(
        "%s.csv" % config_table_name))


def get_config_table_schema_file(base_dir: str, config_table_name: str) -> str:
    return (Path(base_dir).joinpath(CONFIG_TABLES_SCHEMA_DIR).joinpath(
        "%s_schema.json" % config_table_name))


def get_table_schema(source_schema_file: str) -> List:
    with open(source_schema_file) as json_file:
        data = json.load(json_file)
        schema = [SchemaField.from_api_repr(json_field) for json_field in data]
    return schema


def update_or_create_table_from_csv(
        client: bigquery.Client,
        table_name: str,
        source_file: str,
        dataset: str,
        source_schema_file: str,
):
    LOGGER.debug("update_or_create_table_from_csv: %s=%s", table_name,
                 [source_file])
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table_name)

    job_config = LoadJobConfig()
    job_config.source_format = "CSV"
    job_config.skip_leading_rows = 1
    if Path(source_schema_file).exists():
        job_config.schema = get_table_schema(source_schema_file)
    else:
        job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    with open(source_file, "rb") as source_fp:
        load_job = client.load_table_from_file(source_fp,
                                               destination=table_ref,
                                               job_config=job_config)

    # wait for job to complete
    load_job.result()

    LOGGER.info("updated config table: %s", table_ref.table_id)


def update_or_create_config_tables(client: bigquery.Client, base_dir: str,
                                   config_table_names: List[str],
                                   dataset: str):
    LOGGER.info("config_table_names: %s", config_table_names)
    for config_table_name in config_table_names:
        update_or_create_table_from_csv(
            client,
            config_table_name,
            get_config_table_file(base_dir, config_table_name),
            dataset=dataset,
            source_schema_file=get_config_table_schema_file(
                base_dir, config_table_name),
        )
