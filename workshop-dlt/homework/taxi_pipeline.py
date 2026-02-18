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
    """
    params: dict[str, object] = {
        "dataset": dataset,
    }
    if year is not None:
        params["year"] = year
    if month is not None:
        params["month"] = month

    # Use PageNumberPaginator but explicitly tell it NOT to look for a total count
    paginator = PageNumberPaginator(
        base_page=1,
        page_param="page",
        total_path=None,  # This is the fix! It won't look for a 'total' key.
        maximum_page=max_pages,
        stop_after_empty_page=True, # This stops the loop when it gets []
    )

    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
                "paginator": paginator,
            },
            "resource_defaults": {
                "write_disposition": "replace",
                "endpoint": {
                    "params": params,
                },
            },
            "resources": [
                {
                    "name": "ny_taxi_trips",
                    "endpoint": {
                        "path": "",
                    },
                },
            ],
        }
    )

# Initialize the pipeline
taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    progress="log",
)

if __name__ == "__main__":
    # This will now fetch page 1, page 2... until an empty list is returned
    load_info = taxi_pipeline.run(taxi_rest_api_source())
    print(load_info)