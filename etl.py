"""
ETL Process for Loading Data into Redshift.

This module automates the process of:
1. Generating manifest files for new S3 files not yet loaded into Redshift.
2. Extracting data from S3 into Redshift staging tables.
3. Loading data into the star schema tables from the staging tables.
4. Truncating staging and star schema tables for debugging purposes.

Functions:
----------
- load_staging_tables(cur, conn): Executes queries to load data into staging
  tables from S3.
- insert_tables(cur, conn): Executes queries to load data into star schema
  tables from staging tables.
- truncate_staging(cur, conn): Truncates staging tables for debugging.
- truncate_star_tables(cur, conn): Truncates star schema tables for debugging.
- main(): Orchestrates the ETL process, including configuration, manifest
  generation, and data loading.

Dependencies:
-------------
- configparser: For reading configuration from `dwh.cfg`.
- psycopg2: For connecting and executing queries in Redshift.
- boto3: For interacting with AWS S3.
- logging: For logging ETL operations and timing.
- sql_queries: Contains the SQL queries for the ETL process.
- aws_utils: Provides helper functions like `aws_generate_manifest`.

Execution:
----------
This script is executed as a standalone program. Ensure the `dwh.cfg` file is
configured with the appropriate AWS and Redshift credentials and settings.

Example:
--------
To run the script:
    $ python etl.py
"""

import configparser
import psycopg2
import boto3
import time
import logging
from sql_queries import copy_table_queries, insert_table_queries, \
    truncate_staging_table_queries, truncate_star_table_queries
from aws_utils import aws_generate_manifest

logging.basicConfig(
    filename='logs/etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_staging_tables(cur, conn):
    """
    Load data into Redshift staging tables from S3.

    This function iterates through a list of SQL COPY queries, executes
    each query to load data into the staging tables, and commits the
    transactions.

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.

    Returns:
    --------
    None
        The function logs the execution time of each query and commits
        the results to Redshift.
    """
    for query in copy_table_queries:
        start_time = time.time()
        logger.info(f"Loading Staging table with query: {query}")
        cur.execute(query)
        conn.commit()
        end_time = time.time()
        logger.info(f'staging query completed in \
                    {end_time - start_time:.2f} seconds')


def insert_tables(cur, conn):
    """
    Load data into Redshift star schema tables from staging tables.

    This function iterates through a list of SQL INSERT queries, executes
    each query to populate the star schema tables, and commits the transactions

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.

    Returns:
    --------
    None
        The function logs each query being executed and commits the results
        to Redshift.
    """
    for query in insert_table_queries:
        logger.info(f"Loading Star tables with query: {query}")
        cur.execute(query)
        conn.commit()
    return None


def truncate_staging(cur, conn):
    """
    Truncate Redshift staging tables.

    This function iterates through a list of SQL TRUNCATE queries and executes
    each query to remove all data from the staging tables. The transactions
    are committed after execution.

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.

    Returns:
    --------
    None
        The function removes all data from the staging tables by executing
        the provided TRUNCATE queries.
    """
    for query in truncate_staging_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def truncate_star_tables(cur, conn):
    """
    Truncate Redshift star schema tables.

    This function iterates through a list of SQL TRUNCATE queries and executes
    each query to remove all data from the star schema tables. The transactions
    are committed after execution.

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.

    Returns:
    --------
    None
        The function removes all data from the star schema tables by executing
        the provided TRUNCATE queries.
    """
    for query in truncate_star_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def main():
    """
    Orchestrate the ETL process for loading data into Redshift.

    This function performs the following steps:
    1. Reads configuration variables from a `dwh.cfg` file.
    2. Generates manifest files for new S3 files not yet loaded into Redshift.
    3. Loads data from S3 into Redshift staging tables.
    4. Populates Redshift star schema tables from the staging tables.
    5. Truncates staging and star schema tables (optional, for debugging).

    The process uses helper functions to perform individual tasks like
    generating manifests, loading staging tables, and inserting data into
    the star schema tables.

    Args:
    -----
    None

    Returns:
    --------
    None
        This function orchestrates the entire ETL process and logs relevant
        information, including execution times, at each step.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Load config vars
    AWS_KEY = config.get("AWS", "KEY")
    AWS_SECRET = config.get("AWS", "SECRET")
    AWS_REGION = config.get("AWS", "REGION")
    DATA_BUCKET = config.get("S3", "DATA_BUCKET")
    LOG_DATA = config.get("S3", "LOG_DATA")
    SONG_DATA = config.get("S3", "SONG_DATA")
    REDSHIFT_HOST = config.get("CLUSTER", "HOST")
    REDSHIFT_DB_NAME = config.get("CLUSTER", "DB_NAME")
    REDSHIFT_DB_USER = config.get("CLUSTER", "DB_USER")
    REDSHIFT_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    REDSHIFT_DB_PORT = config.get("CLUSTER", "DB_PORT")
    MANIFEST_BUCKET = config.get("S3", "MANIFEST_BUCKET")
    MANIFEST_SONG = config.get("S3", "MANIFEST_SONG")
    MANIFEST_LOG = config.get("S3", "MANIFEST_LOG")

    # Generate manifests ------------------------------------------------------
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    # song-logs
    logger.info("fetching song log manifest")
    start_time = time.time()
    aws_generate_manifest(s3,
                          data_bucket=DATA_BUCKET,
                          file_prefix=LOG_DATA,
                          redshift_host=REDSHIFT_HOST,
                          port=REDSHIFT_DB_PORT,
                          dbname=REDSHIFT_DB_NAME,
                          user=REDSHIFT_DB_USER,
                          password=REDSHIFT_DB_PASSWORD,
                          table_name="staging_song_log",
                          manifest_bucket=MANIFEST_BUCKET,
                          manifest_key=MANIFEST_LOG)
    end_time = time.time()
    logger.info(f'Song log manifest completed in {end_time - start_time:.2f} \
                seconds')
    # song-data
    logger.info('Fetching song data manifest')

    start_time = time.time()
    aws_generate_manifest(s3,
                          data_bucket=DATA_BUCKET,
                          file_prefix=SONG_DATA,
                          redshift_host=REDSHIFT_HOST,
                          port=REDSHIFT_DB_PORT,
                          dbname=REDSHIFT_DB_NAME,
                          user=REDSHIFT_DB_USER,
                          password=REDSHIFT_DB_PASSWORD,
                          table_name="staging_song_data",
                          manifest_bucket=MANIFEST_BUCKET,
                          manifest_key=MANIFEST_SONG,
                          limit=None)
    end_time = time.time()
    logger.info(f'Song data manifest completed in {end_time - start_time:.2f} \
                seconds')

    # Extract data from S3 and put it into Redshift staging -------------------
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER']
                                    .values()))
    cur = conn.cursor()
    logger.info('beginning staging')
    start_time = time.time()
    truncate_staging(cur, conn)  # used for debug
    load_staging_tables(cur, conn)
    end_time = time.time()
    logger.info(f'Staging tables loaded in {end_time - start_time:.2f} \
                seconds')

    # Load data into the star schema from the staging_tables
    logger.info('beginning inserts')
    start_time = time.time()
    truncate_star_tables(cur, conn)  # used for debug
    insert_tables(cur, conn)
    logger.info('completed insert actions')
    end_time = time.time()
    logger.info(f'Insert actions completed in {end_time - start_time:.2f} \
                seconds')
    conn.close()
    return None


if __name__ == "__main__":
    """
    Entry point for the ETL process.

    This block ensures that the `main` function is executed only when the
    script is run directly, not when it is imported as a module in another
    script.
    """
    main()
