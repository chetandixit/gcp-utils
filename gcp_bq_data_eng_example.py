"""
Author: Chetan Dixit
This code sample covers few concepts for Data Engineering with Google Bigquery.
To make this code work, create a service account on GCP IAM Console,
assign data owner and bigquery job user role to this service account.
Create a key for this service account and download the json file.

Concepts covered:
1. connecting to Bigquery programmatically with a service account.
2. Execute a query using pandas-gbq package
3. retrieve the results in a dataframe
4. create a transformation function and apply it on dataframe
5. Write the transformed dataframe to CSV File
6. Write the transformed dataframe to Bigquery Tables
7. perform aggregates and write aggregated results to Bigquery Tables

This program uses bigquery-public-data, new_york_taxi_trips dataset and tlc_yellow_trips tables
It retrieves latitude and longitude of pickup location.
Transformation function get_zipcode finds out the zipcode based on lat and long
Results are stored in file and table
You should create a dataset named nyc_trip_data_mart
Replace PROJECT_ID and SERVICE_ACCOUNT_KEY_FILE_PATH with your values
path to file on windows need to use forward slash (/) like C:/Users/abcd/yourfile.json
"""

import geopy
import pandas as pd
import pandas_gbq as bq
from google.oauth2 import service_account

PROJECT_ID = "YOUR_GCP_PROJECT_ID"
SERVICE_ACCOUNT_KEY_FILE_PATH = "PATH_TO_YOUR_JSON_KEY_FILE"
QUERY_TEXT = """SELECT 
                vendor_id, 
                pickup_longitude, 
                pickup_latitude  
                FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2016` 
                WHERE pickup_longitude is not null 
                and pickup_latitude is not null
                and pickup_longitude != 0.0
                and pickup_latitude != 0.0
                LIMIT 200"""


# Function to get the zipcode from latitude and longitude
# Function can be applied to a dataframe
def get_zipcode(df, geolocator, lat_field, lon_field):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    return location.raw['address']['postcode']


# Initialize geopy
geo_locator = geopy.Nominatim(user_agent='xyz_app')
# Get the Google Cloud Credentials for Bigquery
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_KEY_FILE_PATH)
# Run the query on Bigquery using pandas-gbq, returns a dataframe
result = bq.read_gbq(query=QUERY_TEXT,
                     project_id=PROJECT_ID,
                     credentials=credentials)
# Apply the get_zipcode function to the dataframe, note no iteration
# A new column is getting added to dataframe with name zipcode
result['zipcode'] = result.apply(get_zipcode,
                                 axis=1,
                                 geolocator=geo_locator,
                                 lat_field='pickup_latitude',
                                 lon_field='pickup_longitude')
# Write the result dataframe to a csv file
result.to_csv('result_with_zipcodes.csv', index=False)
# You could write the result dataframe in one go but I putting an example of creation and append
# Write dataframe to a Bigquery table (only first 100 rows). New table gets created
bq.to_gbq(result[:100], 'nyc_trip_data_mart.temp_table', project_id=PROJECT_ID)
# Write next 100 record i.e. append to an existing table
bq.to_gbq(result[100:], 'nyc_trip_data_mart.temp_table', project_id=PROJECT_ID, if_exists='append')
# Aggregate: trip counts for each zipcode: select zipcode, count(vendor_id) from table group by zipcode
agg_result = (result.groupby('zipcode', as_index=False)['vendor_id'].count()).rename(columns = {'vendor_id': 'trip_count'})
bq.to_gbq(agg_result, 'nyc_trip_data_mart.temp_table_agg', project_id=PROJECT_ID, if_exists='replace')
