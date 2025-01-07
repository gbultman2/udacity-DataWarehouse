#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 08:34:45 2024

@author: greg
"""


import psycopg2
import json

def s3_list_bucket_files(s3, bucket, file_prefix, limit = None):
    """
    Args
    ----------
    s3 : boto3.client('s3') you need to specify key and secret 
    bucket : Name of the s3 Bucket
    file_prefix : prefix to filter files

    Returns
    -------
    files : List of file keys

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


def redshift_get_committed_files(redshift_host, port, dbname, user, password, table_name, prefix):
    """
    Find the committed files in redshift

    Args:
    -------
        redshift_host: Redshift cluster endpoint.
        port: Port number.
        dbname: Redshift database name.
        user: Database username.
        password: Database password.
        table_name: Table to check committed files.
        prefix: Prefix for the files (e.g., song-data, log-data).

    Returns:
        List of committed file names.
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

def compare_files(redshift_committed_files, s3_files):
    """
    Args
    ----------
    redshift_committed_files : file listing of files committed to redshift
    s3_files : files in the s3 bucket

    Returns
    -------
    New file listing of files to load

    """
    return list(set(s3_files) - set(redshift_committed_files))

def s3_create_manifest(s3, data_bucket, manifest_bucket, manifest_key, file_list):
    """
    Args
    ----------
    s3 : s3 client (specify region, key, secret key)
    bucket_name : the bucket that has the files to upload
    manifest_bucket: the bucket where the manifest will go
    manifest_key : the key for the manifest file
    file_list : listing of files that need to be loaded (from compare_files)
        DESCRIPTION.
        
    Manifest contents looks like this:
    {
      "entries": [
        {"url": "s3://your-bucket/path/to/file1", "mandatory": true},
        {"url": "s3://your-bucket/path/to/file2", "mandatory": true}
      ]
    }
    """
    manifest_content = {
            "entries": [{"url": f"{file}", "mandatory": True} for file in file_list]
        }
    manifest_json = json.dumps(manifest_content, indent=2)
    
    s3.put_object(Bucket=manifest_bucket,
                  Key=manifest_key,
                  Body=manifest_json,
                  ContentType="application/json")
    print(f"Manifest uploaded at: s3://{manifest_bucket}/{manifest_key}")
        # Return the manifest's S3 URI
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
                          limit = None):
    """
    This function compares the files in the data bucket to the system table in
    redshift to see which files are already loaded.  It then creates a manifest
    in the manifest_bucket to list files that need to be added to the redshift
    database.
    
    args
    ----------
    s3 : s3 client
    data_bucket: name of the bucket where data is stored
    file_prefix: prefix of the file to be loaded e.g. song-data log-data
    redshift_host: hostname for the redshift cluster
    port: port for the redshift cluster
    dbname: redshift database
    user: redshift user name
    password: redshift password
    table_name: table name of the staging table to check against
    manifest_bucket: the bucket where the manifest is stored
    manifest_key: the key (filename) of the manifest json
    limit: limits the files retrieved by s3 - used for troubleshooting

    Returns
    -------
    None.

    """
    s3_files = s3_list_bucket_files(s3,
                                    data_bucket,
                                    file_prefix, 
                                    limit = limit)
    s3_files_with_prefix = [f"s3://{data_bucket}/{file}" for file in s3_files]
    redshift_committed_files = redshift_get_committed_files(redshift_host,
                                                            port,
                                                            dbname,
                                                            user,
                                                            password,
                                                            table_name,
                                                            prefix = file_prefix)
    
    
    new_files = compare_files(redshift_committed_files, 
                              s3_files_with_prefix)
    
    s3_create_manifest(s3,
                       data_bucket = data_bucket,
                       manifest_bucket = manifest_bucket,
                       manifest_key = manifest_key,
                       file_list = new_files)
    return None