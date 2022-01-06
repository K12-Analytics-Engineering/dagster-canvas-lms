from dagster import repository

from jobs.canvas import canvas_dev_job

@repository
def repository():
    return [
        canvas_dev_job
    ]
