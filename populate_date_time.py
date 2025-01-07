#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 12:56:41 2024

@author: greg
"""

import pandas as pd
import os

# Generate the time dimension data
def create_dim_time_csv():
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
    create_dim_date_csv()
    create_dim_time_csv()
    upload_to_s3(s3, bucket_name)
    
