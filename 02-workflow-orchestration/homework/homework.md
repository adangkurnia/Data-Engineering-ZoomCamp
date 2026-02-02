1. Within the execution for `Yellow` Taxi data for the year `2020` and month `12`: what is the uncompressed file size (i.e. the output file `yellow_tripdata_2020-12.csv` of the `extract` task)?
    
    **Answer: 134.5 MB**
    
    <img width="480" height="34" alt="image" src="https://github.com/user-attachments/assets/c9dbf4d7-f9d8-418e-bae4-b4744437d2d7" />

    
2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution? (1 point)
    
    **Answer: green_tripdata_2020-04.csv**
    
    <img width="385" height="303" alt="image" src="https://github.com/user-attachments/assets/dc77c970-3fef-4c2e-9ccc-e0c92a81fc61" />

    
3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020? (1 point)
    
    **Answer: 24,648,499**
    
    ```sql
    SELECT 
      SUM(row_count) AS total_row_count
    FROM `project-1a58d016-57b1-4ae2-b06.zoomcamp.__TABLES__`
    WHERE table_id LIKE '%yellow%' 
      AND table_id LIKE '%2020%'
    ```
    
4. How many rows are there for the Green Taxi data for all CSV files in the year 2020? (1 point)
    
    **Answer: 1,734,051**
    
    ```sql
    SELECT 
      SUM(row_count) AS total_row_count
    FROM `project-1a58d016-57b1-4ae2-b06.zoomcamp.__TABLES__`
    WHERE table_id LIKE '%green%' 
      AND table_id LIKE '%2020%'
    ```
    
5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file? (1 point)
    
    **Answer: 1,925,152**
    
    ```sql
    SELECT COUNT(*) 
    FROM `project-1a58d016-57b1-4ae2-b06.zoomcamp.yellow_tripdata_2021_03`
    ```
    
6. How would you configure the timezone to New York in a Schedule trigger? (1 point)
    
    **Answer: Add a timezone property set to America/New_York in the Schedule trigger configuration**
