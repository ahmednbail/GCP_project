'''
This file should contain the needed code blocks for reading the data stored on GCS, cleaning it, then loading it into BigQuery.

Cleaning may contain:
    - Flattening the JSON files
    - Removing the meaningless nulls
    - Changing data types
    - Check for duplicates

Note: Please be sure that you've created the dataset (movies_raw_data) and tables (with the same names as the files) on BigQuery before loading the results.

Here you'll use google-cloud-storage client library to read files from GCS, and google-cloud-bigquery or pandas_gbq to load data into BigQuery.

Helpful resources:
------------------
1. https://docs.cloud.google.com/appengine/docs/legacy/standard/python/googlecloudstorageclient/read-write-to-cloud-storage
2. https://docs.cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
'''