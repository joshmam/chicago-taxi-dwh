import csv
import requests
import pandas as pd
import s3fs
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

import os

# Get credentials
chicago_app_token = os.getenv('chicago_app_token')
chicago_app_secret = os.getenv('chicago_app_secret')
aws_key = os.getenv('aws_key')
aws_secret = os.getenv('aws_secret')

# Set AWS S3 file system
fs = s3fs.S3FileSystem(key=aws_key, secret=aws_secret)

# BASE FUNCTIONS

def last_df_row_to_tuple(df):
    last_row_df = df[-1:][['trip_start_timestamp', 'trip_id']]
    cutoff_ts = last_row_df.iloc[0]['trip_start_timestamp'][:-4]
    cutoff_id = last_row_df.iloc[0]['trip_id']
    return (cutoff_ts, cutoff_id)

def make_df_cols_consistent(df, column_order):
    for col in column_order:
        if col not in df.columns:
            df[col] = None
    df = df[column_order]
    return df

# DATA HANDLING FUNCTIONS

# Note - still contains static file path
def store_cutoff_state(cutoff_tuple):
    try:
        with fs.open('s3://josh-chicago-data/dev/last_batch_cutoff.csv', 'w', newline='') as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow(cutoff_tuple)
    except (TypeError, ValueError):
        print(f"Error: ")


# Note - still contains static file path. To do: make filepath dynamic
def retrieve_cutoff_state():
    with fs.open('s3://josh-chicago-data/dev/last_batch_cutoff.csv', 'r', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            last_batch_cutoff = tuple(row)
            break
    return last_batch_cutoff


# Note - this generates a query_string that is specific only to Chicago Taxi dataset
def generate_api_query_string(last_timestamp, last_trip_id, this_batch_size):
    return f"?$query=SELECT * WHERE trip_start_timestamp >= '{last_timestamp}' AND (trip_id > '{last_trip_id}' OR trip_start_timestamp != '{last_timestamp}') ORDER BY trip_start_timestamp ASC, trip_id ASC LIMIT {this_batch_size}"


def query_soda_api(base_url, query_string, app_token):
    url = base_url + query_string

    headers = {
        'Accept': 'application/json',
        'X-App-Token': chicago_app_token
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error: {str(e)}")
        
    data = response.json()
    return pd.DataFrame.from_records(data)


def save_data_to_S3_csv(df, file_path, output_filename):
    try:
        with fs.open(file_path + output_filename + '.csv', 'w') as file:
            df.to_csv(file, index=False, line_terminator='\n')
    except (TypeError, ValueError):
        print(f"Error: ")
        
        
# MAIN FUNCTION

def run_connector(connector_definitions, batch_size):
    try:
        for conn in connector_definitions:
            this_base_url = None
            this_file_path = None
            this_file_prefix = None
            this_columns_order = None
            cutoff_timestamp = None
            cutoff_id = None
            api_query_string = None
            df = None
            df_consistent = None
            
            this_base_url = conn['soda_base_url']
            this_file_path = conn['dest_S3_path']
            this_file_prefix = conn['dest_file_prefix']
            this_columns_order = conn['dest_columns_order']

            # retrieve the stored cutoff state
            cutoff_timestamp, cutoff_id = retrieve_cutoff_state()

            # generate the query string for extracting the next batch of data
            api_query_string = generate_api_query_string(cutoff_timestamp, cutoff_id, batch_size)

            # get the data from the API
            df = query_soda_api(this_base_url, api_query_string, chicago_app_token)

            df_row_count = len(df)
            
            # store the data as a csv file in S3 if API response contains data
            if df_row_count > 0:
                print(f"{df_row_count} rows to load.")
                now_str = None
                filename = None
                latest_cutoff_state = None

                now_str = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
                filename = f"{ this_file_prefix }_{ now_str }"
                df_consistent = make_df_cols_consistent(df, this_columns_order)
                save_data_to_S3_csv(df_consistent, this_file_path, filename)

                # update the last_batch_cutoff file
                latest_cutoff_state = last_df_row_to_tuple(df)
                store_cutoff_state(latest_cutoff_state)
                print(latest_cutoff_state)
            else:
                print("No rows to load.")

        print("Connector ran successfully.")
    except Exception as e:
        print(f"Connector failed. Error: {e}")


# Set the connector definitions - maps 1 or more SODA APIs

# To do: move this to a config file
column_order = ['trip_id',
                'taxi_id',
                'trip_start_timestamp',
                'trip_end_timestamp',
                'trip_seconds',
                'trip_miles',
                'pickup_community_area',
                'dropoff_community_area',
                'fare',
                'tips',
                'tolls',
                'extras',
                'trip_total',
                'payment_type',
                'company',
                'pickup_centroid_latitude',
                'pickup_centroid_longitude',
                'pickup_centroid_location',
                'dropoff_centroid_latitude',
                'dropoff_centroid_longitude',
                'dropoff_centroid_location',
                'pickup_census_tract',
                'dropoff_census_tract']


# To do: move this to a config file
connector_definitions_list = [
                                {
                                     'soda_base_url': 'https://data.cityofchicago.org/resource/wrvz-psew.json',
                                     'dest_S3_path': 's3://josh-chicago-data/dev/batch_extracts/',
                                     'dest_file_prefix': 'raw_taxi_trips',
                                     'dest_columns_order': column_order
                                }
                            ]


# To do: move this to an input
batch_size = 50000

def lambda_handler(event, context):

    try:
        # RUN THE CONNECTOR
        run_connector(connector_definitions_list, batch_size)

    except Exception as e:
        # Send some context about this error to Lambda Logs
        print(e)
        raise e

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Success"
        })
    }