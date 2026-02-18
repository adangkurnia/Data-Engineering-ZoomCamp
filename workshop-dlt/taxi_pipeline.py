"""Pipeline to ingest NYC taxi trips from the Zoomcamp REST API."""

import dlt
from dlt.sources.rest_api import rest_api_source


def nyc_taxi_source():
    """Create a dlt source for the NYC taxi REST API (paginated JSON)."""
    return rest_api_source(
        {
            "client": {
                "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/",
            },
            "resource_defaults": {
                "write_disposition": "replace",
            },
            "resources": [
                {
                    "name": "nyc_taxi_trips",
                    "endpoint": {
                        "path": "data_engineering_zoomcamp_api",
                        "params": {
                            "limit": 1000,
                        },
                        "data_selector": "$",
                        "paginator": {
                            "type": "page_number",
                            "base_page": 1,
                            "page_param": "page",
                            "stop_after_empty_page": True,
                        },
                    },
                }
            ],
        }
    )


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="taxi_data",
        progress="log",
    )

    load_info = pipeline.run(nyc_taxi_source())
    print(load_info)

