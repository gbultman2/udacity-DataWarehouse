"""
Module: check_data.py.

This script retrieves and lists files from AWS S3 buckets based on specified
prefixes. It uses the `s3_list_bucket_files` function from the `aws_utils`
module to fetch the file keys for log data and song data stored in an S3
bucket.

Configuration:
--------------
Reads AWS credentials and S3 bucket information from `dwh.cfg`:
- AWS_KEY: AWS access key.
- AWS_SECRET: AWS secret key.
- AWS_REGION: AWS region.
- DATA_BUCKET: Name of the S3 bucket containing the data.
- LOG_DATA: Prefix for log data files in the bucket.
- SONG_DATA: Prefix for song data files in the bucket.

Dependencies:
-------------
- configparser: For reading configuration from `dwh.cfg`.
- boto3: For interacting with AWS S3.
- aws_utils: Provides the `s3_list_bucket_files` function to list files in S3.

Execution:
----------
When executed, the script connects to AWS S3, retrieves file keys for log data
and song data based on their prefixes, and assigns them to `song_logs` and
`song_data` variables, respectively.

"""
import configparser
import boto3
from aws_utils import s3_list_bucket_files

config = configparser.ConfigParser()
config.read('dwh.cfg')

AWS_KEY = config.get("AWS", "KEY")
AWS_SECRET = config.get("AWS", "SECRET")
AWS_REGION = config.get("AWS", "REGION")
DATA_BUCKET = config.get("S3", "DATA_BUCKET")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


s3 = boto3.client('s3',
                  aws_access_key_id=AWS_KEY,
                  aws_secret_access_key=AWS_SECRET,
                  region_name=AWS_REGION)

song_logs = s3_list_bucket_files(s3,
                                 bucket=DATA_BUCKET,
                                 file_prefix=LOG_DATA)
song_data = s3_list_bucket_files(s3,
                                 bucket=DATA_BUCKET,
                                 file_prefix=SONG_DATA)
