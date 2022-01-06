from typing import List, Dict

import requests
from dagster import get_dagster_logger, resource
from tenacity import retry, wait_exponential


class CanvasApiClient:
    """Class for interacting with Canvas API"""


    def __init__(self, api_base_url, api_access_token, account_id):
        self.api_base_url=api_base_url
        self.api_access_token=api_access_token
        self.account_id=account_id
        self.log=get_dagster_logger()


    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def _call_api(self, url) -> Dict:
        """
        Call GET on passed in URL and
        return response.
        """
        headers={"Authorization" : f"Bearer {self.api_access_token}"}
        self.log.debug(url)
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.warn("Failed to retrieve data")
            # a 404 will be returned if an assignment is deleted
            # after fetching the assignment ids and before fetching
            # submissions
            if response.status_code == 404:
                self.log.debug(response.text)
            else:
                raise err
        
        return response.json()


    def get_terms(self) -> List:
        """
        Get terms data from Canvas API
        and return JSON data
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/accounts/{self.account_id}/terms?page=1&per_page=100"
        )
        response = self._call_api(endpoint_url)
        terms = response["enrollment_terms"]
        self.log.info(f"Retrieved {len(terms)} records")
        return terms



@resource(
    config_schema={
        "api_base_url": str,
        "api_access_token": str,
        "account_id": str
    },
    description="A Canvas LMS client that retrieves data from their restful API.",
)
def canvas_api_resource_client(context):
    return CanvasApiClient(
        context.resource_config["api_base_url"],
        context.resource_config["api_access_token"],
        context.resource_config["account_id"]
    )
