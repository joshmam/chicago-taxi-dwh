-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the 'chicago' user and assign to role
CREATE USER IF NOT EXISTS chicago
  PASSWORD='<removed>'
  LOGIN_NAME='chicago'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='CHICAGO.RAW'
  COMMENT='Chicago user used for data transformation';
GRANT ROLE transform to USER chicago;

-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS CHICAGO;
CREATE SCHEMA IF NOT EXISTS CHICAGO.RAW;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE CHICAGO to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE CHICAGO to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE CHICAGO to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA CHICAGO.RAW to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA CHICAGO.RAW to ROLE transform;

show roles
show grants to role transform