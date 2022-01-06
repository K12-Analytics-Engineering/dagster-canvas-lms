import csv
import json
import uuid
from typing import Dict, List

import pandas as pd
from dagster import get_dagster_logger, resource
from google.cloud import exceptions, storage


class GcsClient:
    """Class for interacting with Google Cloud Storage"""

    def __init__(self, gcs_bucket, gcs_prefix):
        self.gcs_bucket = gcs_bucket
        self.gcs_prefix = gcs_prefix
        self.client = storage.Client()
        self.log = get_dagster_logger()
        try:
            self.bucket = self.client.get_bucket(self.gcs_bucket)
        except exceptions.NotFound:
            self.log.error("Sorry, that bucket does not exist!")
            raise

    def upload(self, file_name: str, df: pd.DataFrame) -> str:
        """
        Upload dataframe to GCS as CSV
        and return GCS folder path.
        """
        self.log.debug(f"Uploading {file_name} to gs://{self.gcs_bucket}/{self.gcs_prefix}")

        self.bucket.blob(
            f"{self.gcs_prefix}/{file_name}").upload_from_string(
                df.to_csv(
                    index=False,
                    quoting=csv.QUOTE_ALL
                ),
                content_type="text/csv",
                num_retries=3)

        return f"gs://{self.gcs_bucket}/{self.gcs_prefix}/{file_name}"


    def upload_json(self, folder_name, records) -> List[str]:
        gcs_paths = list()

        # delete existing files
        blobs = self.bucket.list_blobs(prefix=f"{self.gcs_prefix}/{folder_name}/")
        for blob in blobs:
            blob.delete()

        # upload records into 10,000 record JSON chunks
        self.log.info(f"Splitting {len(records)} into 10,000 record chunks.")
        for i in range(0, len(records), 10000):
            gcs_file = f"{self.gcs_prefix}/{folder_name}/{str(uuid.uuid4())}.json"
            output = ""
            for record in records[i:i+10000]:
                output = output + json.dumps(record) + '\r\n'

            self.bucket.blob(gcs_file).upload_from_string(
                output,
                content_type="application/json",
                num_retries=3
            )

            gcs_paths.append(gcs_file)

        self.log.debug(gcs_paths)
        return gcs_paths


@resource(
    config_schema={
        "gcs_bucket": str,
        "gcs_prefix": str,
    },
    description="Google Cloud Storage client",
)
def gcs_client(context):
    """
    Initialize and return GcsClient()
    """
    return GcsClient(
        context.resource_config["gcs_bucket"],
        context.resource_config["gcs_prefix"]
    )
