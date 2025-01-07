import configparser
import psycopg2
import boto3
import logging

from sql_queries import create_table_queries, drop_table_queries, populate_datetime_queries
from populate_date_time import s3_upload_date_time

logging.basicConfig(
    filename='logs/create_tables.log', 
    level=logging.INFO,      
    format='%(asctime)s - %(levelname)s - %(message)s'  
)
logger = logging.getLogger(__name__)  # Create a logger instance


def drop_tables(cur, conn):
    for query in drop_table_queries:
        logger.info(f"Dropping table with query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn, s3, bucket):
    for query in create_table_queries:
        logger.info(f"Creating table with query: {query}")  
        cur.execute(query)
        conn.commit()
        # populate date time tables
    logger.info("Uploading date-time tables to S3")  
    s3_upload_date_time(s3, bucket) # requires that the user have S3 PUT access
    for query in populate_datetime_queries:
        logger.info(f"Populating date-time table with query: {query}")  
        cur.execute(query)
        conn.commit()


def main():
    logger.info("Starting table creation process") 
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    AWS_KEY=config.get("AWS", "KEY")
    AWS_SECRET = config.get("AWS", "SECRET")
    AWS_REGION = config.get("AWS", "REGION")
    MANIFEST_BUCKET = config.get("S3", "MANIFEST_BUCKET")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    s3 = boto3.client('s3',
                      aws_access_key_id=AWS_KEY, 
                      aws_secret_access_key=AWS_SECRET, 
                      region_name=AWS_REGION)
    # Drop Table
    drop_tables(cur, conn)
    # Create Table
    create_tables(cur, conn, s3, bucket = MANIFEST_BUCKET)    
    conn.close()
    logger.info("Tables created successfully")  

if __name__ == "__main__":
    main()