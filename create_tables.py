"""
Module: create_tables.py.

This script automates the process of managing Redshift tables and populating
date-time tables for an ETL pipeline. It performs the following tasks:
1. Drops existing tables in Redshift (if they exist).
2. Creates new tables in Redshift.
3. Populates date-time tables and uploads them to S3 for further use.

Functions:
----------
- drop_tables(cur, conn): Drops existing tables in Redshift.
- create_tables(cur, conn, s3, bucket): Creates new tables, populates date-time
  tables, and uploads them to S3.
- main(): Orchestrates the entire process by configuring connections and
  invoking the table management functions.

Dependencies:
-------------
- configparser: Reads configuration values from `dwh.cfg`.
- psycopg2: Connects to and manages the Redshift database.
- boto3: Interacts with AWS S3.
- logging: Logs actions and execution details for debugging and tracking.
- sql_queries: Contains SQL queries for creating, dropping, and populating
  tables.
- populate_date_time: Contains the `s3_upload_date_time` function for uploading
  date-time tables to S3.

Execution:
----------
To run the script:
    $ python create_tables.py
"""


import configparser
import psycopg2
import boto3
import logging

from sql_queries import create_table_queries, drop_table_queries, \
    populate_datetime_queries
from populate_date_time import s3_upload_date_time

logging.basicConfig(
    filename='logs/create_tables.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)  # Create a logger instance


def drop_tables(cur, conn):
    """
    Drop existing tables in Redshift.

    This function iterates through a list of SQL DROP queries, executes each
    query to remove the corresponding table in Redshift, and commits the
    transaction.

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.

    Returns:
    --------
    None
        The function logs each table being dropped and commits the changes.
    """
    for query in drop_table_queries:
        logger.info(f"Dropping table with query: {query}")
        cur.execute(query)
        conn.commit()
    return None


def create_tables(cur, conn, s3, bucket):
    """
    Create new tables in Redshift and populate date-time tables.

    This function iterates through a list of SQL CREATE queries, executes each
    query to create the corresponding table in Redshift, and commits the
    transactions. It also populates date-time tables, uploads them to S3, and
    populates them in Redshift.

    Args:
    -----
    cur : psycopg2.extensions.cursor
        A database cursor object for executing SQL queries.
    conn : psycopg2.extensions.connection
        A database connection object for committing transactions.
    s3 : boto3.client
        An initialized S3 client for uploading date-time tables.
    bucket : str
        Name of the S3 bucket to which the date-time tables will be uploaded.

    Returns:
    --------
    None
        The function logs each table being created, uploads date-time tables
        to S3, and populates the date-time tables in Redshift.
    """
    for query in create_table_queries:
        logger.info(f"Creating table with query: {query}")
        cur.execute(query)
        conn.commit()
        # populate date time tables
    logger.info("Uploading date-time tables to S3")
    s3_upload_date_time(s3, bucket)  # require that the user have S3 PUT access
    for query in populate_datetime_queries:
        logger.info(f"Populating date-time table with query: {query}")
        cur.execute(query)
        conn.commit()
    return None


def main():
    """
    Orchestrate the Redshift table creation process.

    This function performs the following steps:
    1. Reads configuration values from `dwh.cfg` to set up Redshift and S3
       connections.
    2. Drops existing tables in Redshift.
    3. Creates new tables, populates date-time tables, and uploads them to S3.

    Args:
    -----
    None

    Returns:
    --------
    None
        The function logs the progress and ensures successful table management.
    """
    logger.info("Starting table creation process")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    AWS_KEY = config.get("AWS", "KEY")
    AWS_SECRET = config.get("AWS", "SECRET")
    AWS_REGION = config.get("AWS", "REGION")
    MANIFEST_BUCKET = config.get("S3", "MANIFEST_BUCKET")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER']
                                    .values()))
    cur = conn.cursor()
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_KEY,
                      aws_secret_access_key=AWS_SECRET,
                      region_name=AWS_REGION)
    # Drop Table
    drop_tables(cur,
                conn)
    # Create Table
    create_tables(cur,
                  conn,
                  s3, bucket=MANIFEST_BUCKET)
    conn.close()
    logger.info("Tables created successfully")
    return None


if __name__ == "__main__":
    """
    Entry point for the Redshift table creation process.

    This block ensures that the `main` function is executed only when the
    script is run directly, not when it is imported as a module in another
    script.
    """
    main()
