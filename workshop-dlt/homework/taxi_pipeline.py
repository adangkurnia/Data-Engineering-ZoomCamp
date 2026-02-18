"""`dlt` REST API pipeline to ingest NYC taxi data."""

import dlt
from dlt.sources.rest_api import rest_api_source


def taxi_rest_api_source(
    year: int | None = None,
    month: int | None = None,
    dataset: str = "ny_taxi",
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

    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",  # noqa: E501
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
                        "paginator": {
                            "type": "page_number",
                            "page_param": "page",
                            "start_page": 1,
                        },
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
