-- Setup Integration to S3

CREATE OR REPLACE STORAGE INTEGRATION AWS_S3_integration_chicago
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::206138242773:role/snowflake_role_chicago_db'
  STORAGE_ALLOWED_LOCATIONS = ('s3://josh-chicago-data/dev/', 's3://josh-chicago-data/prod/')
-- NOTE: if the STORAGE INTEGRATION is replaced then the AWS service user ARN and External ID are regenerated
-- Get these by running "DESC integration <integration_name>", then update them in AWS -> IAM -> Roles -> <role> -> Trusted Relationships

DESC integration AWS_S3_integration_chicago;

USE DATABASE chicago;
GRANT CREATE STAGE ON SCHEMA raw TO ROLE transform;
GRANT USAGE ON INTEGRATION AWS_S3_integration_chicago TO ROLE transform;

USE SCHEMA raw;

-- Setup the External Stage

CREATE OR REPLACE STAGE s3_stage_raw_chicago
  STORAGE_INTEGRATION = AWS_S3_integration_chicago
  URL = 's3://josh-chicago-data/dev/'
  FILE_FORMAT = (type = 'CSV' skip_header = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"');

GRANT ALL PRIVILEGES ON STAGE s3_stage_raw_chicago TO ROLE transform;

DESC STAGE s3_stage_raw_chicago;

LIST @s3_stage_raw_chicago