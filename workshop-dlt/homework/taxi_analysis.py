import marimo

__generated_with = "0.19.11"
app = marimo.App()


@app.cell
def _():
    import ibis

    con = ibis.duckdb.connect("taxi_pipeline.duckdb")
    t = con.table("ny_taxi_trips", database="nyc_taxi_data")
    return (t,)


@app.cell
def _(t):
    # 1. Start and end date of the dataset
    start_end = t.aggregate(
        start_date=t.trip_pickup_date_time.min(),
        end_date=t.trip_dropoff_date_time.max(),
    )
    start_end
    return


@app.cell
def _(t):
    # 2. Proportion of trips paid with credit card
    counts = t.group_by(t.payment_type).aggregate(n=t.count())
    counts_df = counts.execute()
    credit_trips = counts_df.loc[counts_df["payment_type"] == "Credit", "n"].sum()
    total_trips = counts_df["n"].sum()
    credit_proportion = f"{(credit_trips / total_trips)*100}%" if total_trips else 0.0
    credit_proportion
    return 


@app.cell
def _(t):
    # 3. Total amount of money generated in tips
    tips_total = t.tip_amt.sum().round(2).execute()
    tips_total
    return


if __name__ == "__main__":
    app.run()
