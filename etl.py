import configparser
import psycopg2
import boto3
import time
import logging
from sql_queries import copy_table_queries, insert_table_queries, truncate_staging_table_queries, truncate_star_table_queries
from aws_utils import aws_generate_manifest

logging.basicConfig(
    filename='logs/etl.log', 
    level=logging.INFO,      
    format='%(asctime)s - %(levelname)s - %(message)s'  
)
logger = logging.getLogger(__name__)  # Create a logger instance


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        start_time = time.time()
        logger.info(f"Loading Staging table with query: {query}")  
        cur.execute(query)
        conn.commit()
        end_time = time.time()
        logger.info(f'staging query completed in {end_time - start_time:.2f} seconds')  


def insert_tables(cur, conn):
    for query in insert_table_queries:
        logger.info(f"Loading Star tables with query: {query}") 
        cur.execute(query)
        conn.commit()

def truncate_staging(cur, conn):
    for query in truncate_staging_table_queries:
        cur.execute(query)
        conn.commit()
        
def truncate_star_tables(cur, conn):
    for query in truncate_star_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # Load config vars
    AWS_KEY = config.get("AWS", "KEY")
    AWS_SECRET = config.get("AWS", "SECRET")
    AWS_REGION = config.get("AWS", "REGION")
    DATA_BUCKET = config.get("S3", "DATA_BUCKET")
    LOG_DATA=config.get("S3", "LOG_DATA")
    SONG_DATA =config.get("S3", "SONG_DATA")
    REDSHIFT_HOST=config.get("CLUSTER", "HOST")
    REDSHIFT_DB_NAME=config.get("CLUSTER", "DB_NAME")
    REDSHIFT_DB_USER=config.get("CLUSTER", "DB_USER")
    REDSHIFT_DB_PASSWORD=config.get("CLUSTER", "DB_PASSWORD")
    REDSHIFT_DB_PORT=config.get("CLUSTER", "DB_PORT")
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
                          data_bucket = DATA_BUCKET,
                          file_prefix = LOG_DATA,
                          redshift_host = REDSHIFT_HOST,
                          port=REDSHIFT_DB_PORT, 
                          dbname=REDSHIFT_DB_NAME,
                          user=REDSHIFT_DB_USER,
                          password=REDSHIFT_DB_PASSWORD,
                          table_name="staging_song_log",
                          manifest_bucket=MANIFEST_BUCKET,
                          manifest_key=MANIFEST_LOG)
    end_time = time.time()
    logger.info(f'Song log manifest completed in {end_time - start_time:.2f} seconds')
    
    # song-data
    logger.info('Fetching song data manifest')
    
    start_time = time.time()
    aws_generate_manifest(s3, 
                          data_bucket = DATA_BUCKET,
                          file_prefix = SONG_DATA,
                          redshift_host = REDSHIFT_HOST,
                          port=REDSHIFT_DB_PORT, 
                          dbname=REDSHIFT_DB_NAME,
                          user=REDSHIFT_DB_USER,
                          password=REDSHIFT_DB_PASSWORD,
                          table_name="staging_song_data",
                          manifest_bucket=MANIFEST_BUCKET,
                          manifest_key=MANIFEST_SONG, 
                          limit = None)
    end_time = time.time()
    logger.info(f'Song data manifest completed in {end_time - start_time:.2f} seconds')
    
    # Extract data from S3 and put it into Redshift staging -------------------
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    logger.info('beginning staging')
    start_time = time.time()
    truncate_staging(cur, conn) # used for debug
    load_staging_tables(cur, conn)
    end_time = time.time()
    logger.info(f'Staging tables loaded in {end_time - start_time:.2f} seconds')
    
    # Load data into the star schema from the staging_tables
    logger.info('beginning inserts')
    start_time = time.time()
    truncate_star_tables(cur, conn) # used for debug
    insert_tables(cur, conn)
    logger.info('completed insert actions')
    end_time = time.time()
    logger.info(f'Insert actions completed in {end_time - start_time:.2f} seconds')
    conn.close()


if __name__ == "__main__":
    main()