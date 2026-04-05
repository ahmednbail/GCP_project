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

import base64
import json
import functions_framework
from google.cloud import bigquery, storage
import pandas as pd
import io

# ============================================
# CONFIG
# ============================================
PROJECT_ID = "project-dc54b7ee-7ed4-4dda-8a0"
BUCKET_NAME = "gcp_projec"
DATASET_ID = "project-dc54b7ee-7ed4-4dda-8a0.movies_raw_data"
OUTPUT_DATASET_ID = "project-dc54b7ee-7ed4-4dda-8a0.movies_insightful_information"  # where to save results

# ============================================
# TRIGGERED BY PUBSUB
# ============================================
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    print(f"Received Pub/Sub message: {message}")

    # Run the pipeline
    df_movies, df_credits = read_from_bigquery()
    json_data = read_from_gcs()

    df_movies, df_credits, df_json = clean_data(df_movies, df_credits, json_data)

    results = transform_data(df_movies, df_credits, df_json)

    save_results_to_bigquery(results)

    print("Pipeline completed successfully!")


# ============================================
# 1. READ FROM BIGQUERY
# ============================================
def read_from_bigquery():
    client = bigquery.Client(project=PROJECT_ID)

    print("Reading from BigQuery...")

    movies_query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.movies`
    """

    credits_query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET_ID}.credits`
    """

    df_movies  = client.query(movies_query).to_dataframe()
    df_credits = client.query(credits_query).to_dataframe()

    print(f"Movies rows: {len(df_movies)}")
    print(f"Credits rows: {len(df_credits)}")

    return df_movies, df_credits


# ============================================
# 2. READ JSON FILES FROM GCS
# ============================================
def read_from_gcs():
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)

    print("📦 Reading JSON files from GCS...")

    all_data = []
    blobs = client.list_blobs(BUCKET_NAME, prefix="json/")

    for blob in blobs:
        if blob.name.endswith(".json"):
            content = blob.download_as_text()
            data = json.loads(content)

            # Handle both list and dict JSON
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)

            print(f"Read: {blob.name}")

    df_json = pd.DataFrame(all_data)
    print(f"JSON total rows: {len(df_json)}")

    return df_json


# ============================================
# 3. CLEANING
# ============================================
def clean_data(df_movies, df_credits, df_json):
    print("🧹 Cleaning data...")

    # ---------- Clean Movies ----------
    # Flatten JSON columns
    for col in ["genres", "production_companies", "spoken_languages"]:
        if col in df_movies.columns:
            df_movies[col] = df_movies[col].apply(lambda x: flatten_json_col(x, "name"))

    # Remove meaningless nulls
    df_movies.dropna(subset=["id", "title"], inplace=True)
    df_movies.fillna({
        "revenue": 0,
        "budget": 0,
        "vote_average": 0.0,
        "vote_count": 0
    }, inplace=True)

    # Change data types
    df_movies["id"]           = df_movies["id"].astype(int)
    df_movies["budget"]       = df_movies["budget"].astype(float)
    df_movies["revenue"]      = df_movies["revenue"].astype(float)
    df_movies["vote_average"] = df_movies["vote_average"].astype(float)

    # Extract year from release_date
    if "release_date" in df_movies.columns:
        df_movies["release_date"] = pd.to_datetime(df_movies["release_date"], errors="coerce")
        df_movies["release_year"] = df_movies["release_date"].dt.year

    # Remove duplicates
    df_movies.drop_duplicates(subset=["id"], inplace=True)
    print(f"Movies after cleaning: {len(df_movies)}")

    # ---------- Clean Credits ----------
    for col in ["cast", "crew"]:
        if col in df_credits.columns:
            df_credits[col] = df_credits[col].apply(lambda x: flatten_json_col(x, "name"))

    df_credits.dropna(subset=["id"], inplace=True)
    df_credits["id"] = df_credits["id"].astype(int)
    df_credits.drop_duplicates(subset=["id"], inplace=True)
    print(f"Credits after cleaning: {len(df_credits)}")

    # ---------- Clean JSON ----------
    df_json.dropna(how="all", inplace=True)
    df_json.drop_duplicates(inplace=True)
    print(f"JSON after cleaning: {len(df_json)}")

    return df_movies, df_credits, df_json


def flatten_json_col(value, key):
    """Extract list of names from JSON string column"""
    try:
        if isinstance(value, str):
            value = json.loads(value.replace("'", '"'))
        if isinstance(value, list):
            return ", ".join([item[key] for item in value if key in item])
    except Exception:
        pass
    return ""


# ============================================
# 4. TRANSFORMATIONS
# ============================================
def transform_data(df_movies, df_credits, df_json):
    print("⚙️ Transforming data...")

    # Join movies + credits
    df = df_movies.merge(df_credits, on="id", how="left")

    results = {}

    # 1. Total revenue per genre
    genre_revenue = (
        df.assign(genre=df["genres"].str.split(", "))
        .explode("genre")
        .groupby("genre", as_index=False)["revenue"]
        .sum()
        .rename(columns={"revenue": "total_revenue"})
        .sort_values("total_revenue", ascending=False)
    )
    results["revenue_per_genre"] = genre_revenue
    print("Total revenue per genre done")

    # 2. Number of movies released per year
    movies_per_year = (
        df.groupby("release_year", as_index=False)["id"]
        .count()
        .rename(columns={"id": "movie_count"})
        .sort_values("release_year")
    )
    results["movies_per_year"] = movies_per_year
    print("Movies per year done")

    # 3. Budget and total revenue per year
    budget_revenue_per_year = (
        df.groupby("release_year", as_index=False)
        .agg(total_budget=("budget", "sum"), total_revenue=("revenue", "sum"))
        .sort_values("release_year")
    )
    results["budget_revenue_per_year"] = budget_revenue_per_year
    print("Budget and revenue per year done")

    # 4. Production companies and total revenue
    company_revenue = (
        df.assign(company=df["production_companies"].str.split(", "))
        .explode("company")
        .groupby("company", as_index=False)
        .agg(total_revenue=("revenue", "sum"), avg_rating=("vote_average", "mean"))
        .sort_values("total_revenue", ascending=False)
    )
    results["company_revenue"] = company_revenue
    print(" Production companies revenue done")

    # 5. Average rating per director
    if "crew" in df.columns:
        df["director"] = df["crew"].apply(extract_director)

        avg_rating_per_director = (
            df[df["director"] != ""]
            .groupby("director", as_index=False)["vote_average"]
            .mean()
            .rename(columns={"vote_average": "avg_rating"})
            .sort_values("avg_rating", ascending=False)
        )
        results["avg_rating_per_director"] = avg_rating_per_director
        print("Average rating per director done")

    return results


def extract_director(crew_str):
    """Extract director name from crew JSON string"""
    try:
        if isinstance(crew_str, str):
            crew = json.loads(crew_str.replace("'", '"'))
            for member in crew:
                if member.get("job") == "Director":
                    return member.get("name", "")
    except Exception:
        pass
    return ""


# ============================================
# 5. SAVE RESULTS TO BIGQUERY
# ============================================
def save_results_to_bigquery(results):
    client = bigquery.Client(project=PROJECT_ID)

    for table_name, df in results.items():
        table_id = f"{PROJECT_ID}.{OUTPUT_DATASET_ID}.{table_name}"

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
            source_format=bigquery.SourceFormat.CSV,
        )

        # Convert to CSV in memory instead of using pyarrow
        csv_data = df.to_csv(index=False)

        job = client.load_table_from_file(
            io.StringIO(csv_data),
            table_id,
            job_config=job_config
        )
        job.result()

        print(f"✅ Saved: {table_name} → {table_id} ({len(df)} rows)")