import json
from typing import Dict, List

import pandas as pd
from dagster import (DynamicOut, DynamicOutput, ExpectationResult, Out, Output,
                     RetryPolicy, op)
from google.cloud import bigquery


@op(
    description="Yields dynamic outputs containing each course id",
    out=DynamicOut(int)
)
def course_id_generator(context, courses: List[Dict]) -> List:
    """
    Dynamically output each course id to allow
    for downstream course related data to be
    extracted in parallel.

    Args:
        courses List[Dict]:
            courses is a list of the courses fetched for
            each Canvas term. Each list item is a dict
            containing a value key that holds the actual
            records retrieved.

            ie. [{"folder_name": "courses", "value": List of records},
                 {"folder_name": "courses", "value": List of records}]
    """
    for course_list in courses:
        for course in course_list["value"]:
            id = course["id"]
            yield DynamicOutput(
                value=id,
                mapping_key=str(id)
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
        {"folder_name": "assignments", "table_name": "canvas_assignments"},
        {"folder_name": "courses", "table_name": "canvas_courses"},
        {"folder_name": "enrollments", "table_name": "canvas_enrollments"},
        {"folder_name": "sections", "table_name": "canvas_sections"},
        {"folder_name": "submissions", "table_name": "canvas_submissions"},
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
    description="Retrieves all assignments for a specific course",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_assignments(context, course_id: int) -> List:
    """
    Retrieve all assignments for a specific course
    """
    records = context.resources.canvas_api_client.get_assignments(course_id)
    yield Output(
        value={
            "course_id": str(course_id),
            "folder_name": "assignments",
            "value": records
        },
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Retrieves all courses configured for specified account",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_courses(context, term_id: int) -> List:
    """
    Retrieve all courses for a specific term
    """
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
    description="Retrieves all enrollments for a specific course",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_enrollments(context, course_id: int) -> List:
    """
    Retrieve all enrollments for a specific course
    """
    records = context.resources.canvas_api_client.get_enrollments(course_id)
    yield Output(
        value={
            "folder_name": "enrollments",
            "value": records
        },
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Retrieves all sections for a specific course",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_sections(context, course_id: int) -> List:
    """
    Retrieve all sections for a specific course
    """
    records = context.resources.canvas_api_client.get_sections(course_id)
    yield Output(
        value={
            "folder_name": "sections",
            "value": records
        },
        metadata={
            "record_count": len(records)
        }
    )


@op(
    description="Retrieves all assignment and quiz submissions",
    required_resource_keys={"canvas_api_client"},
    retry_policy=RetryPolicy(max_retries=3, delay=10),
    tags={"kind": "extract"}
)
def get_submissions(context, assignments: Dict) -> List:
    """
    Loop through all assignments in a course,
    fetching their submissions, and return
    a list containing all submissions for that course.

    Args:
        assignments Dict:
            assignments is a list of the assignments fetched for
            each Canvas course. Each list item is a dict
            containing a value key that holds the actual
            records retrieved.

            ie. [{
                "course_id: id of course assignments pertain to,
                "folder_name": "assignments",
                "value": List of records}]
    """
    records = list()
    course_id = assignments["course_id"]
    for assignment in assignments["value"]:
        if assignment["is_quiz_assignment"] is True:
            quizzes = context.resources.canvas_api_client.get_submissions(
                course_id=course_id,
                assignment_id=str(assignment["quiz_id"]),
                assignment_type="quiz"
            )
            records = records + quizzes["quiz_submissions"]
        else:
            records = records + context.resources.canvas_api_client.get_submissions(
                course_id=course_id,
                assignment_id=str(assignment["id"]),
                assignment_type="assignment"
            )

    yield Output(
        value={
            "folder_name": "submissions",
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
