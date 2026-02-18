"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://openlibrary.org/",
        },
        "resources": [
            {
                "name": "harry_potter_books",
                "endpoint": {
                    "path": "search.json",
                    "params": {
                        "q": "harry potter",
                        "limit": 100,
                        # Open Library Search API defaults to a restricted field set.
                        # Request explicit fields to make the output predictable.
                        "fields": ",".join(
                            [
                                "key",
                                "title",
                                "author_name",
                                "first_publish_year",
                                "edition_count",
                                "isbn",
                                "cover_i",
                                "language",
                                "subject",
                            ]
                        ),
                    },
                    "data_selector": "docs",
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        # Open Library provides `numFound` (total hits), not total pages.
                        # Let pagination stop after an empty page.
                        "total_path": None,
                    },
                },
            }
        ],
        # set `resource_defaults` to apply configuration to all endpoints
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
