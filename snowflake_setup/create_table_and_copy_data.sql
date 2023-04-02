-- Set the defaults
USE WAREHOUSE COMPUTE_WH;
USE DATABASE chicago;
USE SCHEMA RAW;

-- Create tables and import the data from S3
CREATE OR REPLACE TABLE raw_taxi_trips
                        (
                        trip_id string,
                        taxi_id string,
                        trip_start_timestamp string,
                        trip_end_timestamp string,
                        trip_seconds number(6,0),
                        trip_miles number(6,2),
                        pickup_community_area number(4,0),
                        dropoff_community_area number(4,0),
                        fare number(6,2),
                        tips number(6,2),
                        tolls number(6,2),
                        extras number(6,2),
                        trip_total number(6,2),
                        payment_type string,
                        company string,
                        pickup_centroid_latitude number(11,8),
                        pickup_centroid_longitude number(11,8),
                        pickup_centroid_location string,
                        dropoff_centroid_latitude number(11,8),
                        dropoff_centroid_longitude number(11,8),
                        dropoff_centroid_location string,
                        pickup_census_tract string,
                        dropoff_census_tract string
                         );


-- Loads a specified csv file from S3 folder "batch_extracts/"
COPY INTO CHICAGO.RAW.RAW_TAXI_TRIPS
	FROM @CHICAGO.RAW.s3_stage_raw_chicago/batch_extracts/
    file_format = 	(   type=csv 
                        field_delimiter = ','
                        skip_header=1,
                        FIELD_OPTIONALLY_ENCLOSED_BY='"'
                    )
    files = ('raw_taxi_trips_2023-04-01T22-42-31.csv')
    validation_mode = RETURN_ERRORS;

    
-- Loads all matching csv files in S3 folder "batch_extracts/"
COPY INTO CHICAGO.RAW.RAW_TAXI_TRIPS
	FROM @CHICAGO.RAW.s3_stage_raw_chicago/batch_extracts/
    file_format = 	(   type=csv 
                        field_delimiter = ','
                        skip_header=1,
                        FIELD_OPTIONALLY_ENCLOSED_BY='"'
                    )
    pattern = '.*raw_taxi_trips.*\\.csv'
    validation_mode = RETURN_ERRORS;

    
-- Create a pipe for Snowpipe
CREATE OR REPLACE pipe CHICAGO.RAW.chicago_taxis_pipe
auto_ingest = TRUE
AS
COPY INTO CHICAGO.RAW.RAW_TAXI_TRIPS
FROM @CHICAGO.RAW.s3_stage_raw_chicago/batch_extracts/

DESC pipe chicago_taxis_pipe