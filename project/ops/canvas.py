import json
from typing import List, Dict

from dagster import (
    ExpectationResult,
    op,
    Out,
    Output,
    RetryPolicy
)
from google.cloud import bigquery


@op(
    description="Retrieves all terms configured for specified account",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10)
)
def get_terms(context) -> Dict:
    records = context.resources.canvas_api_client.get_terms()
    yield ExpectationResult(success=len(records) > 0, description="ensure records are returned")
    yield Output(
        value={
            "folder_name": "terms",
            "value": records
        },
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Persist extract to data lake",
    required_resource_keys={"file_manager"},
    tags={"kind": "load"},
)
def load_extract(context, extract: Dict) -> List[str]:
    """
    Upload extract to Google Cloud Storage.
    Return list of GCS file paths.
    """
    records = list()
    for record in extract["value"]:
        records.append({
            "id": None,
            "data": json.dumps(record)
        })
    return context.resources.file_manager.upload_json(
        folder_name=extract["folder_name"],
        records=records
    )


@op(
    description="Create tables in BigQuery to query data lake",
    required_resource_keys={"file_manager", "warehouse"},
    retry_policy=RetryPolicy(max_retries=3, delay=30),
)
def create_warehouse_tables(context):
    """
    Create a folder for each api endpoint
    to store raw JSON.
    """
    bucket_name=context.resources.file_manager.gcs_bucket
    gcs_prefix=context.resources.file_manager.gcs_prefix
    schema = [
        bigquery.SchemaField("id", "STRING", "NULLABLE"),
        bigquery.SchemaField("data", "STRING", "NULLABLE")
    ]
    tables = [
        {"folder_name": "terms", "table_name": "canvas_terms"}
    ]
    for table in tables:
        external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
        external_config.source_uris = [f"gs://{bucket_name}/{gcs_prefix}/{table['folder_name']}/*.json"]
        result = context.resources.warehouse.create_table(
            schema=schema,
            external_config=external_config,
            table_name=table["table_name"]
        )

    return "Created data warehouse tables"
