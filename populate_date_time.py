"""
Module: populate_date_time.py.

This module generates dimension tables for time and date, saves them as CSV
files, and uploads them to an AWS S3 bucket. The time dimension covers
seconds in a day, while the date dimension spans a range of years.

Functions:
----------
- create_dim_time_csv(): Generates a time dimension table with second-level
  granularity and saves it as `dim_time.csv`.
- create_dim_date_csv(): Generates a date dimension table covering a specified
  date range and saves it as `dim_date.csv`.
- upload_to_s3(s3, bucket_name): Uploads the generated `dim_time.csv` and
  `dim_date.csv` files to an S3 bucket.
- s3_upload_date_time(s3, bucket_name): High-level function that generates the
  date and time dimension tables and uploads them to S3.

Dependencies:
-------------
- pandas: For data manipulation and generating time/date dimensions.
- os: For file and directory management.
- boto3: For interacting with AWS S3.

Execution:
----------
The script is designed to be used as part of an ETL pipeline. It can generate
the date and time dimensions and upload them to S3 in one function call
(`s3_upload_date_time`).
"""

import pandas as pd
import os


def create_dim_time_csv():
    """
    Generate a time dimension table and save it as `dim_time.csv`.

    This function generates a time dimension table with second-level
    granularity, covering all 86,400 seconds in a day. The table includes
    columns for time in HH:MM:SS format, hour, minute, second, and AM/PM.

    Returns:
    --------
    None
        Saves the generated time dimension table as `data/dim_time.csv`.
    """
    seconds = range(86400)  # Total seconds in a day: 0 to 86399
    data = {
        "time_key": seconds,  # Total seconds since midnight
        "time_key_sql": [f"{s // 3600:02}:{(s % 3600) // 60:02}:{s % 60:02}" for s in seconds],  # TIME in HH:MM:SS
        "hour": [s // 3600 for s in seconds],  # Hour of the day
        "minute": [(s % 3600) // 60 for s in seconds],  # Minute of the hour
        "second": [s % 60 for s in seconds],  # Second of the minute
        "am_pm": ["AM" if s // 3600 < 12 else "PM" for s in seconds],  # AM or PM based on the hour
    }
    df_time = pd.DataFrame(data)
    os.makedirs("data", exist_ok=True)
    file_path = os.path.join("data", "dim_time.csv")
    df_time.to_csv(file_path, index=False)
    return


def create_dim_date_csv():
    """
    Generate a date dimension table and save it as `dim_date.csv`.

    This function generates a date dimension table for a specified date range
    (2015-01-01 to 2025-12-31). The table includes columns for:
    - `date_key`: A unique key in YYYYMMDD format.
    - `date_key_sql`: The full date in SQL-compatible format.
    - `week`: The ISO week number.
    - `month`: The full name of the month.
    - `year`: The year.
    - `is_weekday`: Indicates whether the date is a weekday or weekend.

    The table is saved as a CSV file in the `data/` directory.

    Returns:
    --------
    None
        Saves the generated date dimension table as `data/dim_date.csv`.
    """
    dates = pd.date_range(start="2015-01-01", end="2025-12-31")
    data = {
        "date_key": dates.strftime("%Y%m%d").astype(int),  # Unique key in YYYYMMDD format
        "date_key_sql": dates,  # Full date
        "week": dates.isocalendar().week,  # ISO week number
        "month": dates.strftime("%B"),  # Full month name
        "year": dates.year,  # Year
        "is_weekday": ["Weekday" if date.weekday() < 5 else "Weekend" for date in dates],  # Weekday or Weekend
    }
    df_date = pd.DataFrame(data)
    file_path = os.path.join("data", "dim_date.csv")
    df_date.to_csv(file_path, index=False)
    return


def upload_to_s3(s3, bucket_name):
    """
    Upload the date and time dimension CSV files to an S3 bucket.

    This function uploads two CSV files, `dim_date.csv` and `dim_time.csv`,
    from the local `data/` directory to the specified S3 bucket.

    Args:
    -----
    s3 : boto3.client
    An initialized S3 client for interacting with AWS S3.
    bucket_name : str
    The name of the S3 bucket where the files will be uploaded.

    Returns:
    --------
    None
    The function uploads the files to the specified S3 bucket without
    returning any values.
    """
    s3.upload_file(
        Filename="./data/dim_date.csv",  # Local file path
        Bucket=bucket_name,             # S3 bucket name
        Key="dim_date.csv"     # S3 object key
    )

    # Upload dim_time.csv
    s3.upload_file(
        Filename="./data/dim_time.csv",  # Local file path
        Bucket=bucket_name,             # S3 bucket name
        Key="dim_time.csv"     # S3 object key
    )


def s3_upload_date_time(s3, bucket_name):
    """
    Generate date and time dimension tables and upload them to an S3 bucket.

    This function:
    1. Generates the date dimension table and saves it as `dim_date.csv`.
    2. Generates the time dimension table and saves it as `dim_time.csv`.
    3. Uploads the generated CSV files to the specified S3 bucket.

    Args:
    -----
    s3 : boto3.client
        An initialized S3 client for interacting with AWS S3.
    bucket_name : str
        The name of the S3 bucket where the files will be uploaded.

    Returns:
    --------
    None
        The function generates and uploads the date and time dimension tables
        to the specified S3 bucket.
    """
    create_dim_date_csv()
    create_dim_time_csv()
    upload_to_s3(s3, bucket_name)
