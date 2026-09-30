# Databricks notebook source
from datetime import date, datetime

# ============================================================
# PARAMS
# ============================================================
date_of_data = dbutils.widgets.get("date_of_data")
run_id = dbutils.widgets.get("run_id")

try:
    date_of_data = datetime.strptime(date_of_data, "%Y-%m-%d").strftime("%Y-%m")
except ValueError:
    raise ValueError(
        f"Uncorrect: '{date_of_data}'. "
        "Expected format: YYYY-MM-DD."
    )

year = date_of_data[:4]
month = date_of_data[5:]

print(year, month)

# COMMAND ----------

import requests
from requests import HTTPError
import re
import os

# ============================================================
# SOURCE
# ============================================================

datasets = ["yellow", "green"]

for ds in datasets:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{ds}_tripdata_{date_of_data}.parquet"

    # ============================================================
    # PARSE FILE NAME
    # ============================================================

    file_name = url.rstrip("/").split("/")[-1]

    match = re.match(
        r"^(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet$",
        file_name
    )

    if not match:
        raise ValueError(
            f"Uncorrect filename TLC: {file_name}"
        )

    taxi_type = match.group(1)
    year = match.group(2)
    month = match.group(3)


    # ============================================================
    # TARGET PATH
    # ============================================================

    base_volume_path = "/Volumes/bronze/dbo/ny_taxi"

    target_dir = (
        f"{base_volume_path}/"
        f"{taxi_type}/"
        f"{year}/"
        f"{month}"
    )

    target_path = f"{target_dir}/{file_name}"

    print(f"Taxi type: {taxi_type}")
    print(f"Year:      {year}")
    print(f"Month:     {month}")
    print(f"File:      {file_name}")
    print(f"Target:    {target_path}")


    # ============================================================
    # CHECK IF FILE EXISTS
    # ============================================================

    if os.path.exists(target_path):

        print(f"File exists yet. Skip: {target_path}")

    else:

        print("The file doesn't exists. Downloading...")

        # --------------------------------------------------------
        # DOWNLOAD
        # --------------------------------------------------------

        try:
            response = requests.get(
                url,
                stream=True,
                timeout=300
            )

            response.raise_for_status()

            # --------------------------------------------------------
            # CREATE DIRECTORY
            # --------------------------------------------------------

            os.makedirs(
                target_dir,
                exist_ok=True
            )

            # --------------------------------------------------------
            # WRITE DIRECTLY TO VOLUME
            # --------------------------------------------------------

            with open(target_path, "wb") as file:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):
                    if chunk:
                        file.write(chunk)

            print(f"Downloaded and saved: {target_path}")

        except HTTPError as e:
            print(f"HTTPError: {e}")
            print(f"Trace ID: {e.response.headers.get('X-Amzn-Trace-Id')}")