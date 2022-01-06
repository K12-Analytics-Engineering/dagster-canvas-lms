import json
from typing import Dict, List

import pandas as pd
from dagster import (DynamicOut, DynamicOutput, ExpectationResult, Out, Output,
                     RetryPolicy, op)
from google.cloud import bigquery


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
        {"folder_name": "courses", "table_name": "canvas_courses"},
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


@op(
    description="Retrieves all courses configured for specified account",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_courses(context, term_id: int) -> List:
    records = context.resources.canvas_api_client.get_courses(term_id)
    yield Output(
        value={
            "folder_name": "courses",
            "value": records
        },
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Retrieves all terms configured for specified account",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_terms(context) -> List:
    records = context.resources.canvas_api_client.get_terms()
    yield ExpectationResult(success=len(records) > 0, description="ensure records are returned")
    yield Output(
        value=[{
            "folder_name": "terms",
            "value": records
        }],
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Persist extract to data lake",
    required_resource_keys={"file_manager"},
    tags={"kind": "load"},
)
def load_data(context, extract: List) -> List[str]:
    """
    Upload extract to Google Cloud Storage.
    Return list of GCS file paths.
    """
    records = list()
    for set_of_records in extract:
        for record in set_of_records["value"]:
            records.append({
                "id": None,
                "data": json.dumps(record)
            })
    return context.resources.file_manager.upload_json(
        folder_name=extract[0]["folder_name"],
        records=records
    )


@op(
    description="Yields dynamic outputs containing each term id",
    config_schema={"school_year_start_date": str},
    out=DynamicOut(int)
)
def term_id_generator(context, terms: List) -> List:
    """
    Load terms extract into a dataframe,
    filter dataframe to only terms on or after
    school year start date, dynamically
    output the term ids
    """
    school_year_start_date = context.op_config["school_year_start_date"]
    df = pd.DataFrame(terms[0]["value"])
    df["start_at"] = pd.to_datetime(df["start_at"])
    current_terms_df = df[(df["start_at"] >= school_year_start_date)]

    for id in current_terms_df['id']:
        yield DynamicOutput(
            value=id,
            mapping_key=str(id)
        )
