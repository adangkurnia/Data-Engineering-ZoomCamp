import marimo as mo


app = mo.App()


@app.cell
def _():
    import ibis

    con = ibis.duckdb.connect("taxi_pipeline.duckdb")
    t = con.table("nyc_taxi_data__ny_taxi_trips")
    return con, t


@app.cell
def _(t):
    # 1. Start and end date of the dataset
    start_end = t.aggregate(
        start_date=t.Trip_Pickup_DateTime.min(),
        end_date=t.Trip_Dropoff_DateTime.max(),
    )
    start_end


@app.cell
def _(t):
    # 2. Proportion of trips paid with credit card
    counts = t.group_by(t.Payment_Type).aggregate(n=t.count())
    counts_df = counts.execute()
    credit_trips = counts_df.loc[counts_df["Payment_Type"] == "Credit", "n"].sum()
    total_trips = counts_df["n"].sum()
    credit_proportion = credit_trips / total_trips if total_trips else 0.0
    credit_proportion


@app.cell
def _(t):
    # 3. Total amount of money generated in tips
    tips_total = t.Tip_Amt.sum().execute()
    tips_total


if __name__ == "__main__":
    app.run()