"""
Module: aws_utils.py.

This module provides helper functions for managing ETL operations in a data
warehouse project. It includes functionalities for interacting with AWS S3,
generating Redshift manifests, and comparing files between S3 and Redshift.

Functions:
-----------
- s3_list_bucket_files: Lists files in an S3 bucket based on a specified
  prefix.
- redshift_get_committed_files: Fetches committed file names from Redshift.
- compare_files: Compares S3 files with Redshift committed files.
- s3_create_manifest: Creates a manifest JSON file for S3 files to be loaded
  into Redshift.
- aws_generate_manifest: Combines the above utilities to identify new files
  and generate a manifest for loading into Redshift.

Usage:
------
The module is designed to be used as a part of a larger ETL pipeline. The main
entry point is the `aws_generate_manifest` function, which orchestrates the
comparison of files and the creation of a manifest.

Dependencies:
-------------
- boto3
- psycopg2
- json
"""

import psycopg2
import json


def s3_list_bucket_files(s3,
                         bucket,
                         file_prefix,
                         limit=None):
    """
    List files in an S3 bucket with a specified prefix.

    Args:
    -----
    s3 : boto3.client
        An initialized S3 client. Ensure the necessary AWS key and secret
        are specified when creating the client.
    bucket : str
        The name of the S3 bucket.
    file_prefix : str
        The prefix used to filter files in the bucket.
    limit : int, optional
        The maximum number of files to return. If None, all files are returned.

    Returns:
    --------
    list of str
        A list of file keys matching the specified prefix in the bucket.

    Raises:
    -------
    Exception
        If an error occurs during the listing of files in the S3 bucket, the
        exception message is printed, and an empty list is returned.
    """
    files = []
    continuation_token = None

    try:
        while True:
            kwargs = {'Bucket': bucket, 'Prefix': file_prefix}
            if continuation_token:
                kwargs['ContinuationToken'] = continuation_token

            response = s3.list_objects_v2(**kwargs)
            files.extend(obj['Key'] for obj in response.get('Contents', []))

            # Check if there are more files to fetch
            if response.get('IsTruncated'):  # True if there are more pages
                continuation_token = response['NextContinuationToken']
            else:
                break
    except Exception as e:
        print(f"Error listing files in bucket {bucket}: {e}")
    if not files:
        print(f"No Files with prefix {file_prefix} {len(files)}")
    else:
        print(f"Found {len(files)} files with prefix {file_prefix}")
    return files[:limit] if limit else files


def redshift_get_committed_files(redshift_host,
                                 port,
                                 dbname,
                                 user,
                                 password,
                                 table_name,
                                 prefix):
    """
    Retrieve a list of committed file names in a Redshift table.

    Args:
    -----
    redshift_host : str
        The endpoint of the Redshift cluster.
    port : int
        The port number for connecting to the Redshift cluster.
    dbname : str
        The name of the Redshift database.
    user : str
        The username for the database connection.
    password : str
        The password for the database connection.
    table_name : str
        The name of the table to query for committed files.
    prefix : str
        The prefix used to filter committed file names
        (e.g., 'song-data', 'log-data').

    Returns:
    --------
    list of str
        A list of committed file names that match the specified prefix.

    Raises:
    -------
    psycopg2.Error
        If an error occurs while connecting to the database or executing the
        query, the exception will propagate.
    """
    # Connect to Redshift
    conn = psycopg2.connect(
        host=redshift_host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    cursor = conn.cursor()

    # Query to get committed files for the prefix
    query = f"""
    SELECT DISTINCT filename
    FROM stl_load_commits
    WHERE filename LIKE '%{prefix}%';
    """
    cursor.execute(query)
    committed_files = [row[0].strip() for row in cursor.fetchall()]

    # Close connection
    cursor.close()
    conn.close()
    return committed_files


def compare_files(redshift_committed_files,
                  s3_files):
    """
    Identify new files in S3 that are not yet committed in Redshift.

    Args:
    -----
    redshift_committed_files : list of str
        A list of file names that are already committed in Redshift.
    s3_files : list of str
        A list of file names available in the S3 bucket.

    Returns:
    --------
    list of str
        A list of file names present in S3 but not yet committed in Redshift.
    """
    return list(set(s3_files) - set(redshift_committed_files))


def s3_create_manifest(s3,
                       data_bucket,
                       manifest_bucket,
                       manifest_key,
                       file_list):
    """
    Create and upload a manifest file to an S3 bucket.

    Args:
    -----
    s3 : boto3.client
        An initialized S3 client with the appropriate region, key, and secret.
    data_bucket : str
        The name of the S3 bucket containing the data files to upload.
    manifest_bucket : str
        The name of the S3 bucket where the manifest file will be stored.
    manifest_key : str
        The key (filename) for the manifest file to be uploaded.
    file_list : list of str
        A list of file paths (from the `compare_files` function) that need
        to be included in the manifest.

    Manifest Format:
    ----------------
    The manifest file is a JSON object with the following structure:
    {
        "entries": [
            {"url": "s3://your-bucket/path/to/file1", "mandatory": true},
            {"url": "s3://your-bucket/path/to/file2", "mandatory": true}
        ]
    }

    Returns:
    --------
    None
        The function uploads the manifest file to the specified S3 bucket and
        prints the location of the uploaded file.
    """
    manifest_content = {
            "entries": [{"url": f"{file}", "mandatory": True}
                        for file in file_list]
        }
    manifest_json = json.dumps(manifest_content, indent=2)

    s3.put_object(Bucket=manifest_bucket,
                  Key=manifest_key,
                  Body=manifest_json,
                  ContentType="application/json")
    print(f"Manifest uploaded at: s3://{manifest_bucket}/{manifest_key}")

    return None


def aws_generate_manifest(s3,
                          data_bucket,
                          file_prefix,
                          redshift_host,
                          port,
                          dbname,
                          user,
                          password,
                          table_name,
                          manifest_bucket,
                          manifest_key,
                          limit=None):
    """
    Generate a manifest file for new files in S3 that are not yet loaded into\
    Redshift.

    This function compares the files in an S3 data bucket with the files
    already committed in a Redshift table. It identifies new files in S3
    that need to be loaded into Redshift and creates a manifest JSON file
    in the specified S3 manifest bucket.

    Args:
    -----
    s3 : boto3.client
        An initialized S3 client.
    data_bucket : str
        Name of the S3 bucket where data files are stored.
    file_prefix : str
        Prefix used to filter files in the S3 bucket (e.g., 'song-data',
        'log-data').
    redshift_host : str
        Hostname of the Redshift cluster.
    port : int
        Port number for connecting to the Redshift cluster.
    dbname : str
        Name of the Redshift database.
    user : str
        Username for connecting to the Redshift database.
    password : str
        Password for connecting to the Redshift database.
    table_name : str
        Name of the Redshift table to check for committed files.
    manifest_bucket : str
        Name of the S3 bucket where the manifest file will be stored.
    manifest_key : str
        Key (filename) for the manifest JSON file.
    limit : int, optional
        Maximum number of files to retrieve from S3. Used for troubleshooting.

    Returns:
    --------
    None
        This function uploads a manifest file to the specified manifest bucket
        in S3 and prints the location of the manifest file.
    """
    s3_files = s3_list_bucket_files(s3,
                                    data_bucket,
                                    file_prefix,
                                    limit=limit)
    s3_files_with_prefix = [f"s3://{data_bucket}/{file}" for file in s3_files]
    redshift_committed_files = redshift_get_committed_files(redshift_host,
                                                            port,
                                                            dbname,
                                                            user,
                                                            password,
                                                            table_name,
                                                            prefix=file_prefix)

    new_files = compare_files(redshift_committed_files,
                              s3_files_with_prefix)

    s3_create_manifest(s3,
                       data_bucket=data_bucket,
                       manifest_bucket=manifest_bucket,
                       manifest_key=manifest_key,
                       file_list=new_files)
    return None
