'''
This file should contain the needed code blocks for transforming the data that was before loaded into the movies_raw_data dataset,
then make the transformations needed,
and finally save the results into BigQuery again in a separate dataset.

Note: Please be sure that you've created the dataset (movies_insightful_information) and tables (with meaningful names as the aggregations) on BigQuery before loading the results.

The transformations contain:
    - Joining tables
    - Aggregations (you can choose other meaningful aggregations)
        * Total revenue per genre
        * Number of movies released per year
        * Budget and total revenue per year
        * Production companies and average ratings or total revenue
        * Average rating of movies per director

Here you'll use google-cloud-bigquery or pandas_gbq to read and load data into BigQuery.

Helpful resources:
------------------
1. https://cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-read-gbq-simple
2. https://docs.cloud.google.com/bigquery/docs/samples/bigquery-pandas-gbq-to-gbq-simple
'''