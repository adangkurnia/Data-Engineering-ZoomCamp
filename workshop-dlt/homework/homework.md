1. Question 1: What is the start date and end date of the dataset?
    
   <img width="517" height="382" alt="image" src="https://github.com/user-attachments/assets/bb05efca-4d8e-4ba7-98c3-17467bfa7c02" />

    
    ```sql
    # 1. Start and end date of the dataset
    start_end = t.aggregate(
        start_date=t.trip_pickup_date_time.min(),
        end_date=t.trip_dropoff_date_time.max(),
    )
    start_end
    ```
    
2. Question 2: What proportion of trips are paid with credit card?
`26.66%`
    
    ```sql
    # 2. Proportion of trips paid with credit card
    counts = t.group_by(t.payment_type).aggregate(n=t.count())
    counts_df = counts.execute()
    credit_trips = counts_df.loc[counts_df["payment_type"] == "Credit", "n"].sum()
    total_trips = counts_df["n"].sum()
    credit_proportion = f"{credit_trips / total_trips*100}%" if total_trips else 0.0
    credit_proportion
    ```
    
3. Question 3: What is the total amount of money generated in tips?
`6063.41`
    
    ```sql
    # 3. Total amount of money generated in tips
    tips_total = t.tip_amt.sum().round(2).execute()
    tips_total
    ```
