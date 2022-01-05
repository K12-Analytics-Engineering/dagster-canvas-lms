from dagster import repository

from jobs.hello_dagster import hello_dagster_job

@repository
def repository():
    return [
        hello_dagster_job
    ]
