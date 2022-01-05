from dagster import build_op_context

from jobs.hello_dagster import get_name, hello, hello_dagster_job


def test_get_name():
    context = build_op_context()
    result = get_name(context)

    assert result == "dagster"


def test_hello_dagster():
    result = hello_dagster_job.execute_in_process()

    assert result.success
