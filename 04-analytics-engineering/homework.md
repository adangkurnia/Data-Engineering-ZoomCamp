# Module 4 Homework: Analytics Engineering with dbt

In this homework, we'll use the dbt project in `04-analytics-engineering/taxi_rides_ny/` to transform NYC taxi data and answer questions by querying the models.

## Setup

1. Set up your dbt project following the [setup guide](https://www.notion.so/04-analytics-engineering/setup/)
2. Load the Green and Yellow taxi data for 2019-2020 into your warehouse
3. Run `dbt build --target prod` to create all models and run tests

> **Note:** By default, dbt uses the `dev` target. You must use `--target prod` to build the models in the production dataset, which is required for the homework queries below.
> 

After a successful build, you should have models like `fct_trips`, `dim_zones`, and `fct_monthly_zone_revenue` in your warehouse.

---

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `int_trips_unioned` only

<aside>
💡

dbt run --select model run only specific parts of your project instead of everything.

</aside>

---

### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will update the configuration to include the new value

<aside>
💡

09:17:19 [32;1m   Succeeded[0m [   2.52s] test      dbt_akurnia_dbt_test__audit.[1maccepted_values_fct_trips_payment_type__false__1__2__3__4__5__6[0m (test)

</aside>

---

### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,184

<img width="212" height="108" alt="image" src="https://github.com/user-attachments/assets/1db5933f-acd6-4ba2-99a5-9c1838b8c763" />


```sql
SELECT
  COUNT(*)
FROM
  `project-1a58d016-57b1-4ae2-b06.dbt_akurnia.fct_monthly_zone_revenue`;
```

---

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North

<img width="469" height="86" alt="image" src="https://github.com/user-attachments/assets/4e3324f8-59c8-4996-9f44-11e5c0751521" />


```sql
SELECT
pickup_zone, SUM(revenue_monthly_total_amount) AS best_performing_green_zone
FROM
  `project-1a58d016-57b1-4ae2-b06.dbt_akurnia.fct_monthly_zone_revenue` 
WHERE service_type = 'Green'
GROUP BY pickup_zone
ORDER BY SUM(revenue_monthly_total_amount) DESC LIMIT 1;
```

---

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 384,624

<img width="222" height="84" alt="image" src="https://github.com/user-attachments/assets/198df37d-6091-42b6-bafa-32ee58464054" />


```sql
SELECT
SUM(total_monthly_trips) AS total_green_oct_2019_trips
FROM
  `project-1a58d016-57b1-4ae2-b06.dbt_akurnia.fct_monthly_zone_revenue` 
WHERE service_type = 'Green'
AND DATE(revenue_month) BETWEEN '2019-10-01' AND '2019-10-31'; 
```

---

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
    - Filter out records where `dispatching_base_num IS NULL`
    - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 43,244,693

<img width="214" height="75" alt="image" src="https://github.com/user-attachments/assets/9d00978a-c80c-4730-bc02-4ded9309fe3e" />


```sql
SELECT
 COUNT(*)
FROM
  `project-1a58d016-57b1-4ae2-b06.dbt_akurnia.stg_fhv_tripdata`;
```
