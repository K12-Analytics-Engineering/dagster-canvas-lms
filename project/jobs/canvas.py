import os

from dagster import fs_io_manager, graph, multiprocess_executor
from dagster_dbt import dbt_cli_resource
from dagster_gcp.gcs.io_manager import gcs_pickle_io_manager
from dagster_gcp.gcs.resources import gcs_resource
from ops.canvas import create_warehouse_tables, get_terms, load_extract
from resources.bq_resource import bq_client
from resources.canvas_api_resource import canvas_api_resource_client
from resources.gcs_resource import gcs_client


@graph(
    name="canvas",
    description=(
        "Gets data from the Canvas API and"
        "loads to BigQuery. Runs dbt models "
        "after data is loaded."
    )
)
def canvas():

    warehouse_tables_result = create_warehouse_tables()

    terms = get_terms()
    terms_gcs_path = load_extract.alias("load_terms")(terms)




canvas_dev_job = canvas.to_job(
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 8
    }),
    resource_defs={
        "gcs": gcs_resource,
        "file_manager": gcs_client.configured({
            "gcs_bucket": os.getenv("GCS_BUCKET_DEV"),
            "gcs_prefix": "canvas"
        }),
        "io_manager": fs_io_manager,
        "canvas_api_client": canvas_api_resource_client.configured({
            "api_base_url": os.getenv('CANVAS_BASE_URL'),
            "api_access_token": os.getenv('CANVAS_ACCESS_TOKEN'),
            "account_id": "1"
        }),
        "warehouse": bq_client.configured({
            "dataset": "dev_raw_sources",
        }),
        "dbt": dbt_cli_resource.configured({
            "project_dir": os.getenv('DBT_PROJECT_DIR'),
            "profiles_dir": os.getenv('DBT_PROFILES_DIR'),
            "target": "dev"
        })
    }
)
