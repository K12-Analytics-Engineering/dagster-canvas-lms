from typing import List, Dict

from dagster import get_dagster_logger
from dagster import resource
from dagster.config import source
from google.cloud import bigquery
import pandas as pd


class BigQueryClient:
    """Class for interacting with BigQuery"""

    def __init__(self, dataset):
        self.dataset = dataset
        self.client = bigquery.Client()
        self._create_dataset()
        self.dataset_ref = bigquery.DatasetReference(self.client.project, self.dataset)
        self.log = get_dagster_logger()

    def _create_dataset(self):
        """
        Create BigQuery dataset if
        it does not exist.
        """
        self.client.create_dataset(
            bigquery.Dataset(f"{self.client.project}.{self.dataset}"),
            exists_ok=True
        )

    def create_table(self, schema: List,
        external_config: bigquery.ExternalConfig, table_name: str):
        """
        Create BigQuery external table to allow
        dbt to query the data lake
        """
        table_ref = bigquery.Table(self.dataset_ref.table(table_name), schema=schema)
        table_ref.external_data_configuration = external_config
        table = self.client.create_table(table_ref, exists_ok=True)
        self.client.close()
        return f"{table.dataset_id}.{table.table_id}"

    def download_table(self, table_reference: str) -> pd.DataFrame:
        """
        Download table and return the resulting QueryJob.
        """
        table = bigquery.TableReference.from_string(
            f"{self.client.project}.{table_reference}")
        rows = self.client.list_rows(table)
        df = rows.to_dataframe(date_as_object=True)

        self.log.info(f"Downloaded {len(df)} rows from table {table_reference}")

        return df


@resource(
    config_schema={
        "dataset": str,
    },
    description="BigQuery client.",
)
def bq_client(context):
    """
    Initialize and return BigQueryClient()
    """
    return BigQueryClient(
        context.resource_config["dataset"],
    )
