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
    def _call_api(self, url: str, paginate: bool):
        """
        Call GET on passed in URL and
        return response.
        """
        headers={"Authorization" : f"Bearer {self.api_access_token}"}
        self.log.debug(url)
        done = False
        records = list()
        while not done:
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
            
            self.log.info(f"Retrieved {len(response.json())} records")
            if paginate:
                records = records + response.json()
            else:
                return response.json()

            self.log.debug(response.links)
            if "next" in response.links and response.links["current"]["url"] != response.links["next"]["url"]:
                url = response.links["next"]["url"]
            elif "last" in response.links and response.links["current"]["url"] != response.links["last"]["url"]:
                url = response.links["last"]["url"]
            else:
                done = True
        
        return records


    def get_assignments(self, course_id: int) -> List:
        """
        Get assignment data from Canvas API
        and return JSON
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/courses/{course_id}"
            "/assignments?page=1&per_page=100"
        )
        return self._call_api(endpoint_url, True)


    def get_courses(self, term_id: int) -> List:
        """
        Get courses data from Canvas API
        and return JSON
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/accounts/{self.account_id}/courses"
            "?page=1&per_page=100"
            f"&with_enrollments=true&published=true&enrollment_term_id={term_id}"
            "&include[]=term"
            "&include[]=total_students"
            "&include[]=teachers"
        )
        return self._call_api(endpoint_url, True)


    def get_enrollments(self, course_id: int) -> List:
        """
        Get enrollment data from Canvas API
        and return JSON
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/courses/{course_id}"
            "/enrollments?page=1&per_page=100"
            "&include=current_points"
        )
        return self._call_api(endpoint_url, True)


    def get_sections(self, course_id: int) -> List:
        """
        Get section data from Canvas API
        and return JSON
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/courses/{course_id}"
            "/sections?page=1&per_page=100"
            "&include=total_students"
        )
        return self._call_api(endpoint_url, True)


    def get_submissions(self, course_id: str,
        assignment_id: str, assignment_type: str) -> List:
        """
        Get submission data from Canvas API
        and return JSON
        """
        if assignment_type == "assignment":
            endpoint_url = (
                f"{self.api_base_url}"
                f"/api/v1/courses/{course_id}"
                f"/assignments/{assignment_id}/submissions"
                "?page=1&per_page=100"
            )
        else:
            endpoint_url = (
                f"{self.api_base_url}"
                f"/api/v1/courses/{course_id}"
                f"/quizzes/{assignment_id}/submissions"
                "?page=1&per_page=100"
            )

        return self._call_api(endpoint_url, True)


    def get_terms(self) -> List:
        """
        Get terms data from Canvas API
        and return JSON
        """
        endpoint_url = (
            f"{self.api_base_url}"
            f"/api/v1/accounts/{self.account_id}/terms?page=1&per_page=100"
        )
        response = self._call_api(endpoint_url, False)
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
