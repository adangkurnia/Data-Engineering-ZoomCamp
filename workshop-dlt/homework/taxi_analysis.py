import marimo as mo

__generated_with = "0.19.11"
app = mo.App()


@app.cell
def _():
    import ibis

    # DuckDB file created by taxi_pipeline
    con = ibis.duckdb.connect("taxi_pipeline.duckdb")

    # Fully qualified table name; adjust if different
    TABLE = "nyc_taxi_data.ny_taxi_trips"

    return con, TABLE


@app.cell
def _(con, TABLE):
    import marimo as mo

    # 1. Start and end date of the dataset (SQL)
    start_end = mo.sql(
        f"""
        SELECT
          MIN(trip_pickup_date_time) AS start_date,
          MAX(trip_dropoff_date_time) AS end_date
        FROM {TABLE}
        """,
        conn=con.con,
    )
    start_end


@app.cell
def _(con, TABLE):
    import marimo as mo

    # 2. Proportion of trips paid with credit card (SQL)
    credit_prop = mo.sql(
        f"""
        WITH counts AS (
          SELECT
            payment_type,
            COUNT(*) AS n
          FROM {TABLE}
          GROUP BY payment_type
        ),
        totals AS (
          SELECT
            SUM(n) AS total_trips,
            SUM(CASE WHEN payment_type = 'Credit' THEN n ELSE 0 END) AS credit_trips
          FROM counts
        )
        SELECT
          credit_trips * 1.0 / total_trips AS credit_card_proportion
        FROM totals
        """,
        conn=con.con,
    )
    credit_prop


@app.cell
def _(con, TABLE):
    import marimo as mo

    # 3. Total amount of money generated in tips (SQL)
    tips_total = mo.sql(
        f"""
        SELECT
          SUM(tip_amt) AS total_tips
        FROM {TABLE}
        """,
        conn=con.con,
    )
    tips_total


if __name__ == "__main__":
    app.run()
