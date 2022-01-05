from dagster import (
    fs_io_manager,
    graph,
    op
)


@op
def get_name(context) -> str:
    return "dagster"


@op
def hello(context, name: str):
    context.log.info(f"Hello, {name}!")


@graph
def hello_dagster():
    hello(get_name())


hello_dagster_job = hello_dagster.to_job(
    resource_defs={
        "io_manager": fs_io_manager,
    },
)
