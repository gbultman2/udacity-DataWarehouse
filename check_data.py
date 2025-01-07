#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 14:34:07 2024

@author: greg
"""

import pandas as pd
import configparser
import boto3
from aws_utils import s3_list_bucket_files

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_KEY = config.get("AWS", "KEY")
AWS_SECRET = config.get("AWS", "SECRET")
AWS_REGION = config.get("AWS", "REGION")
DATA_BUCKET = config.get("S3", "DATA_BUCKET")
LOG_DATA=config.get("S3", "LOG_DATA")
LOG_JSONPATH=config.get("S3", "LOG_JSONPATH")
SONG_DATA=config.get("S3", "SONG_DATA")


s3 = boto3.client('s3',
                  aws_access_key_id=AWS_KEY, 
                  aws_secret_access_key=AWS_SECRET, 
                  region_name=AWS_REGION)

song_logs = s3_list_bucket_files(s3,
                                 bucket = DATA_BUCKET, 
                                 file_prefix = LOG_DATA)
song_data = s3_list_bucket_files(s3, 
                                 bucket = DATA_BUCKET,
                                 file_prefix = SONG_DATA)
    
    




