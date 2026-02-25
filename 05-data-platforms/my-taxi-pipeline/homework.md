**Module 5 Homework: Data Platforms with Bruin**

In this homework, we'll use Bruin to build a complete data pipeline, from ingestion to reporting.

**Setup**

1. Install Bruin CLI: `curl -LsSf https://getbruin.com/install/cli | sh`
2. Initialize the zoomcamp template: `bruin init zoomcamp my-pipeline`
3. Configure your `.bruin.yml` with a DuckDB connection
4. Follow the tutorial in the [main module README](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-data-platforms)

After completing the setup, you should have a working NYC taxi data pipeline.

---

**Question 1. Bruin Pipeline Structure**

In a Bruin project, what are the required files/directories?

- `.bruin.yml` and pipeline/ with `pipeline.yml` and assets/

```html
In Bruin, project = repository. Your Bruin project is simply your Git repository, and all configuration lives in a single .bruin.yml file at the root of that repository.

The .bruin.yml file must be located in the root directory of your Git repository.
```

```html
A pipeline is a group of assets that are executed together in the right order. For instance, if you have an asset that ingests data from an API, and another one that creates another table from the ingested data, you have a pipeline.

A pipeline is defined with a pipeline.yml file, and all the assets need to be under a folder called assets
```

---

**Question 2. Materialization Strategies**

You're building a pipeline that processes NYC taxi data organized by month based on `pickup_datetime`. Which incremental strategy is best for processing a specific interval period by deleting and inserting data for that time period?

- `time_interval` - incremental based on a time column

```html
time_interval has a behavior to delete rows in date range, then re-insert
```

---

**Question 3. Pipeline Variables**

You have the following variable defined in `pipeline.yml`:

```
variables:
  taxi_types:
    type: array
    items:
      type: string
    default: ["yellow", "green"]
```

How do you override this when running the pipeline to only process yellow taxis?

- `bruin run --var 'taxi_types=["yellow"]'`

```bash
Variables can be overridden at runtime with --var
```

```bash
bruin run 
--start-date 2022-02-24T00:00:00.000Z 
--end-date 2022-02-24T23:59:59.999999999Z 
--var taxi_types='["yellow"]' 
--environment default "c:\Users\Adang Kurnia\Documents\Data-Engineering-ZoomCamp\05-data-platforms\my-taxi-pipeline\assets\raw\trips_raw.py"
```

---

**Question 4. Running with Dependencies**

You've modified the `ingestion/trips.py` asset and want to run it plus all downstream assets. Which command should you use?

- `bruin run ingestion/trips.py --downstream`

```bash
bruin run \
>   --start-date 2022-02-24T00:00:00.000Z \
>   --end-date 2022-02-24T23:59:59.999999999Z \
>   --downstream \
>   --environment default \
>   "c:\Users\Adang Kurnia\Documents\Data-Engineering-ZoomCamp\05-data-platforms\my-taxi-pipeline\assets\raw\trips_raw.py"
```

---

**Question 5. Quality Checks**

You want to ensure the `pickup_datetime` column in your trips table never has NULL values. Which quality check should you add to your asset definition?

- `name: not_null`

---

**Question 6. Lineage and Dependencies**

After building your pipeline, you want to visualize the dependency graph between assets. Which Bruin command should you use?

- `bruin lineage`

---

**Question 7. First-Time Run**

You're running a Bruin pipeline for the first time on a new DuckDB database. What flag should you use to ensure tables are created from scratch?

- `-full-refresh`
