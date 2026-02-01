1. Question 1. What's the version of pip in the python:3.13 image? (1 point)
    
    ```docker
    # Run docker image python:3.13
    docker run -it -- rm --entrypoint=bash python:3.13
    
    # Syntax to check version of pip in the python:3.13 image
    pip --version
    
    output:
    pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)
    ```
    
2. Question 2. Given the docker-compose.yaml, what is the hostname and port that pgadmin should use to connect to the postgres database? (1 point)
    
    ```docker
    services:
      pgdatabase:
        image: postgres:18
        environment:
          POSTGRES_USER: "root"
          POSTGRES_PASSWORD: "root"
          POSTGRES_DB: "ny_taxi"
        volumes:
          - "ny_taxi_postgres_data:/var/lib/postgresql"
        ports:
          - "5432:5432"
    
      pgadmin:
        image: dpage/pgadmin4
        environment:
          PGADMIN_DEFAULT_EMAIL: "admin@admin.com"
          PGADMIN_DEFAULT_PASSWORD: "root"
        volumes:
          - "pgadmin_data:/var/lib/pgadmin"
        ports:
          - "8085:80"
    ```
    
    ```docker
    hostname:pgdatabase
    port:5432
    ```
    
3. Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? (1 point)
    
    ```sql
    SELECT COUNT(TRIP_DISTANCE) AS TOTAL_TRIPS
    FROM PUBLIC.GREEN_TAXI_DATA
    WHERE TRIP_DISTANCE <= 1
    AND lpep_pickup_datetime  
    	BETWEEN 
    		CAST('2025-11-01' AS DATE) 
    	AND 
    		CAST('2025-12-01' AS DATE);
    ```
    
    Output:
    
    8007
    
    <img width="157" height="82" alt="image" src="https://github.com/user-attachments/assets/3af50a45-feac-4b83-8763-ce76ffcf3486" />

    
4. Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles. (1 point)
    
    ```sql
    SELECT 
    	CAST(lpep_pickup_datetime AS DATE ) AS day
    FROM public.green_taxi_data
    WHERE 
    	trip_distance < 100
    ORDER BY trip_distance DESC
    LIMIT 1;
    ```
    
    Output:
    
    2025-11-14
    
    <img width="189" height="82" alt="image" src="https://github.com/user-attachments/assets/256dc5e5-a5de-4554-ab48-0472f5f21880" />

    
5. Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025? (1 point)
    
    ```sql
    WITH largest_pz AS
    (
    SELECT gtd."PULocationID" AS loc_id, count(total_amount) AS total_trips 
    FROM public.green_taxi_data gtd
    WHERE CAST(lpep_pickup_datetime AS DATE ) = DATE('2025-11-18')
    GROUP BY loc_id
    ORDER BY total_trips DESC
    LIMIT 1)
    
    SELECT tz."Zone" from public.taxi_zones tz
    WHERE tz."LocationID" = (SELECT largest_pz."loc_id" FROM largest_pz);
    ```
    
    Output:
    
    East Harlem North
    
    <img width="287" height="86" alt="image" src="https://github.com/user-attachments/assets/fdffdcba-b2a5-4d2b-a406-53a014f7c182" />

    
6. Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? (1 point)
    
    ```sql
    WITH largest_tip AS
    (
    SELECT gtd."DOLocationID" AS do_id, tip_amount
    FROM public.green_taxi_data gtd
    WHERE gtd."PULocationID" = (SELECT tz."LocationID" FROM public.taxi_zones tz WHERE tz."Zone" = 'East Harlem North')
    AND CAST(lpep_pickup_datetime AS DATE ) >= DATE('2025-11-01') 
    ORDER BY tip_amount DESC
    LIMIT 1)
    
    SELECT tz."Zone" FROM public.taxi_zones tz
    WHERE tz."LocationID" = (SELECT "do_id" FROM largest_tip);
    ```
    
    Output:
    
    Yorkville West
    
    <img width="291" height="86" alt="image" src="https://github.com/user-attachments/assets/62ce5cbb-84c3-4f8f-9b76-24c6b727429e" />

    
7. Question 7. Which of the following sequences describes the Terraform workflow for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources? (1 point)
    
    ```sql
    terraform init, terraform apply -auto-approve, terraform destroy
    ```

