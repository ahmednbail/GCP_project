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
from google.cloud import storage, bigquery

# ============================================
# CONFIG
# ============================================
PROJECT_ID = "project-dc54b7ee-7ed4-4dda-8a0"
BUCKET_NAME = "gcp_projec"
DATASET_ID = "project-dc54b7ee-7ed4-4dda-8a0.movies_raw_data"

# Local file paths in Cloud Shell
JSON_FILES = [
    "/home/ahmednab14420/.vscode/GCP_project/GCP Project/data/genres.json",
    "/home/ahmednab14420/.vscode/GCP_project/GCP Project/data/crew.json",
]

CSV_FILES = [
    {"path": "/home/ahmednab14420/.vscode/GCP_project/GCP Project/data/movies.csv", "table": "movies"},
    {"path": "/home/ahmednab14420/.vscode/GCP_project/GCP Project/data/production_companies.csv", "table": "production_companies"},
]

# ============================================
# 1. UPLOAD JSON FILES TO GCS
# ============================================
def upload_json_to_gcs(json_files, bucket_name):
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(bucket_name)

    for file_path in json_files:
        blob = bucket.blob(f"json/{file_path}")
        blob.upload_from_filename(file_path)
        print(f"Uploaded JSON: {file_path} → gs://{bucket_name}/json/{file_path}")

# ============================================
# 2. UPLOAD CSV FILES TO BIGQUERY
# ============================================
def upload_csv_to_bigquery(csv_files, dataset_id):
    client = bigquery.Client(project=PROJECT_ID)

    for file_info in csv_files:
        file_path = file_info["path"]
        table_id = f"{PROJECT_ID}.{dataset_id}.{file_info['table']}"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        with open(file_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()

        table = client.get_table(table_id)
        print(f"Uploaded CSV: {file_path} → {table_id} ({table.num_rows} rows)")

# ============================================
# MAIN
# ============================================

print("Starting upload process...\n")

print("Uploading JSON files to GCS...")
upload_json_to_gcs(JSON_FILES, BUCKET_NAME)

print("\nUploading CSV files to BigQuery...")
upload_csv_to_bigquery(CSV_FILES, DATASET_ID)

