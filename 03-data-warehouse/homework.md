1. What is count of records for the 2024 Yellow Taxi Data? (1 point)
    
    **Answer: 20332093**
    
    ```sql
    SELECT row_count 
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.__TABLES__` 
    WHERE table_id = 'yellow_taxi_2024_view';
    ```
    
2. Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. 
    
    What is the estimated amount of data that will be read when this query is executed on the External Table and the Table? (1 point)
    
    **Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table**
    
    ```sql
    SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationID
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.external_yellow_ny_taxi_2024`;
    
    SELECT COUNT(DISTINCT PULocationID) AS unique_PULocationID
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.yellow_taxi_2024_view`;
    ```
    
     **External Table:**
    
    <img width="795" height="171" alt="image" src="https://github.com/user-attachments/assets/b07bd1bc-a4e5-4423-922e-7b90b5db886c" />

    
    **Materialized Table:**
    
    <img width="728" height="118" alt="image" src="https://github.com/user-attachments/assets/12234199-aafd-44a5-9766-161598f748a3" />

    
3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
    
    **Answer: BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.**
    
    Query one column (PULocationID):
    
    <img width="732" height="143" alt="image" src="https://github.com/user-attachments/assets/a37e6246-f732-437a-88c9-b402005e78a0" />

    
    Query two columns (PULocationID & DOLocationID):
    
    <img width="730" height="81" alt="image" src="https://github.com/user-attachments/assets/983592ca-8fa1-49c3-bf3d-5c3d05a7f422" />

    
4. How many records have a fare_amount of 0? (1 point)
    
    **Answer: 8,333**
    
    ```sql
    SELECT COUNT(fare_amount) as total_fare_amount
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.yellow_taxi_2024_view`
    WHERE fare_amount = 0;
    ```
    
5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy) (1 point)
    
    **Answer: Partition by tpep_dropoff_datetime and Cluster on VendorID**
    
    ```sql
    -- Creating a partition and cluster table
    CREATE OR REPLACE TABLE `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.yellow_tripdata_partitioned_clustered`
    PARTITION BY DATE(tpep_dropoff_datetime)
    CLUSTER BY VendorID AS
    SELECT * FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.external_yellow_ny_taxi_2024`;
    ```
    
6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
    
    Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?Choose the answer which most closely matches.
    
    **Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table**
    
    ```sql
    -- Query scans 310.24 MB
    SELECT COUNT(DISTINCT VendorID) as unique_vendor_id
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.yellow_taxi_2024_view`
    WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
    
    -- Query scans  26.84 MB
    SELECT COUNT(DISTINCT VendorID) as unique_vendor_id
    FROM `project-1a58d016-57b1-4ae2-b06.nyc_taxi_dataset.yellow_tripdata_partitioned_clustered`
    WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
    ```
    
7. Where is the data stored in the External Table you created? (1 point)
    
    **Answer: GCP Bucket**
    
8.  It is best practice in Big Query to always cluster your data: (1 point)
    
    **Answer: False, it depends on the size of the dataset and columns to filter.**
    
9. Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
    
    **Answer: It will estimate 0 bytes will be read.**
    
    <img width="745" height="85" alt="image" src="https://github.com/user-attachments/assets/2f4a9c09-6f78-4bcc-bf66-ba0fa7816354" />

    
    **Why: It will estimate 0 bytes will be read because the materialized table has metadata that stores the information about it, so BigQuery will not go through any single row to count the total rows. Additionally, COUNT(*) doesn't require looking at the values of any specific column (unlike COUNT(VendorID), which would require scanning one column).**
