# QUESTION 1

SELECT COUNT(*) AS record_count
FROM `praxis-wall-411617.week3_green.bq_green_2022`;

CREATE OR REPLACE EXTERNAL TABLE `praxis-wall-411617.week3_green.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://zoomcamp-week3-jm/green_2022']
  );


# QUESTION 2

SELECT COUNT(DISTINCT PULocationID) AS PULocationID_count
FROM `praxis-wall-411617.week3_green.external_green_tripdata`;

SELECT COUNT(DISTINCT PULocationID) AS PULocationID_count
FROM `praxis-wall-411617.week3_green.bq_green_2022`;

#Answer - 0 MB for the External Table and 6.41MB for the Materialized Table


# QUESTION 3

SELECT COUNT(*) AS fare_amount_count
FROM `praxis-wall-411617.week3_green.bq_green_2022`
WHERE fare_amount = 0;


# QUESTION 4

#Have to change datatype from int to timestamp: will do so by creating a new table

CREATE OR REPLACE TABLE `praxis-wall-411617.week3_green.bq_green_2022_timestamps`
AS
SELECT
  *,
  FORMAT_DATETIME('%Y-%m-%d', TIMESTAMP_MICROS(CAST(lpep_pickup_datetime / 1000 AS INT64))) AS pickup_date,
  FORMAT_DATETIME('%Y-%m-%d', TIMESTAMP_MICROS(CAST(lpep_dropoff_datetime / 1000 AS INT64))) AS dropoff_date
FROM
  `praxis-wall-411617.week3_green.bq_green_2022`
WHERE
  lpep_pickup_datetime >= 0
  AND lpep_pickup_datetime <= 2534022143990000000
  AND lpep_dropoff_datetime >= 0
  AND lpep_dropoff_datetime <= 2534022143990000000;

#Got stuck above had had to turn them into a string and ammend the timing abit to get it right with some help from gpt. 

#Below reconverts them into the correct TIMESTAMP... can now move on.

CREATE OR REPLACE TABLE `praxis-wall-411617.week3_green.bq_green_2022_timestamps`
AS
SELECT
  *,
  PARSE_TIMESTAMP('%Y-%m-%d', pickup_date) AS pickup_timestamp1,
  PARSE_TIMESTAMP('%Y-%m-%d', dropoff_date) AS dropoff_timestamp1
FROM
  `praxis-wall-411617.week3_green.bq_green_2022_timestamps`;


#Creating the partitioned and clustered

CREATE OR REPLACE TABLE `praxis-wall-411617.week3_green.part_clust_green_2022`
PARTITION BY
  DATE(pickup_timestamp1)
  CLUSTER BY PULocationID AS
SELECT * FROM `praxis-wall-411617.week3_green.partitioned_green_2022`;
#answer to be parition by lpep_pickup_datetime and cluster by PULocationID

# QUESTION 5

SELECT DISTINCT PULocationID
FROM `praxis-wall-411617.week3_green.bq_green_2022_timestamps`
WHERE
  pickup_timestamp1 >= TIMESTAMP('2022-06-01 00:00:00')
  AND pickup_timestamp1 <= TIMESTAMP('2022-06-30 23:59:59');
#answers: 1.12 MB PARITIONED & 12.82 MB non PARITIONED


# QUESTION 6
#= GCP Bucket


# QUESTION 7
#= False

# QUESTION 8 (BONUS)
SELECT COUNT(*) from `week3_green.part_clust_green_2022`
#= answer - 0, partitioning and clustering reduces load to read data.

