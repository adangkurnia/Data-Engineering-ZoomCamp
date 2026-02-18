"""`dlt` REST API pipeline to ingest NYC taxi data."""

import dlt
from dlt.sources.rest_api import rest_api_source
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator


def taxi_rest_api_source(
    year: int | None = None,
    month: int | None = None,
    dataset: str = "ny_taxi",
    max_pages: int | None = None,
):
    """
    Build a dlt REST API source for the NYC taxi API.

    The API:
    - Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
    - Returns paginated JSON with up to 1,000 records per page
    - Uses page-based pagination and returns an empty list when there is no more data
    """
    params: dict[str, object] = {
        "dataset": dataset,
        # `page` is controlled by the paginator configuration below
    }
    if year is not None:
        params["year"] = year
    if month is not None:
        params["month"] = month

    # The API returns JSON arrays and uses `page` starting at 1.
    paginator = PageNumberPaginator(
        base_page=1,
        page=1,
        page_param="page",
        total_path=None,
        maximum_page=max_pages,
        stop_after_empty_page=True,
    )

    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",  # noqa: E501
                "paginator": paginator,
            },
            "resource_defaults": {
                "write_disposition": "append",
            },
            "resources": [
                {
                    "name": "ny_taxi_trips",
                    "endpoint": {
                        "path": "",
                        "params": params,
                    },
                },
            ],
        }
    )


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)


if __name__ == "__main__":
    # By default, loads all available data for the dataset.
    load_info = taxi_pipeline.run(taxi_rest_api_source())
    print(load_info)  # noqa: T201
