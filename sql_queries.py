import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
AWS_KEY = config.get("AWS", "KEY")
AWS_SECRET = config.get("AWS", "SECRET")
AWS_REGION = config.get("AWS", "REGION")
IAM_ROLE = config.get("IAM", "ARN")
DATA_BUCKET = config.get("S3", "DATA_BUCKET")
LOG_DATA=config.get("S3", "LOG_DATA")
LOG_JSONPATH=config.get("S3", "LOG_JSONPATH")
SONG_DATA=config.get("S3", "SONG_DATA")
MANIFEST_BUCKET = config.get("S3", "MANIFEST_BUCKET")
MANIFEST_SONG = config.get("S3", "MANIFEST_SONG")
MANIFEST_LOG = config.get("S3", "MANIFEST_LOG")

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_song_log"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_song_data"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS dim_song;"
artist_table_drop = "DROP TABLE IF EXISTS dim_artist;"
time_table_drop = "DROP TABLE IF EXISTS dim_time;"
date_table_drop = "DROP TABLE IF EXISTS dim_date;"

# CREATE TABLES
# The staging tables are just from S3 buckets
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_song_log (
    song_log_id BIGINT IDENTITY(1,1) PRIMARY KEY
    ,artist VARCHAR
    ,auth VARCHAR
    ,firstName VARCHAR
    ,gender VARCHAR
    ,itemInSession INT
    ,lastName VARCHAR
    ,length DOUBLE PRECISION
    ,level VARCHAR
    ,location VARCHAR
    ,method VARCHAR
    ,page VARCHAR
    ,registration BIGINT
    ,sessionId INT
    ,song VARCHAR
    ,status INT
    ,ts BIGINT
    ,userAgent VARCHAR
    ,userId INT
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_song_data(
    song_data_id BIGINT IDENTITY(1,1) PRIMARY KEY
    ,num_songs INT
    ,artist_id VARCHAR
    ,artist_latitude DOUBLE PRECISION
    ,artist_longitude DOUBLE PRECISION
    ,artist_location VARCHAR(1000)
    ,artist_name VARCHAR(1000)
    ,song_id VARCHAR
    ,title VARCHAR
    ,duration DOUBLE PRECISION
    ,year INT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS fact_songplay (
    songplay_key BIGINT IDENTITY(1,1) PRIMARY KEY
    ,start_date_key INT
    ,start_time_key INT
    ,user_key INT 
    ,artist_key INT
    ,song_key INT
    ,start_datetime TIMESTAMP 
    , auth VARCHAR
    , session_id BIGINT
    , item_in_session INT
    , method VARCHAR
    , page VARCHAR
    , status INT
    , user_agent VARCHAR
    , songplay_location VARCHAR
    , registration BIGINT
    ,FOREIGN KEY (start_date_key) REFERENCES dim_date(date_key)
    ,FOREIGN KEY (start_time_key) REFERENCES dim_time(time_key)
    ,FOREIGN KEY (user_key) REFERENCES dim_user(user_key)
    ,FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key)
    ,FOREIGN KEY (song_key) REFERENCES dim_song(song_key)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_user (
    user_key BIGINT IDENTITY(1,1) PRIMARY KEY
    ,user_id INT
    ,user_first_name VARCHAR
    ,user_last_name VARCHAR
    ,user_gender VARCHAR(1)
    ,user_level VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_song (
    song_key BIGINT IDENTITY(1,1) PRIMARY KEY
    ,artist_key BIGINT 
    ,song_id VARCHAR
    ,artist_id VARCHAR
    ,song_title VARCHAR
    ,song_year INT
    ,song_duration DOUBLE PRECISION
    ,FOREIGN KEY (artist_key) REFERENCES dim_artist(artist_key)
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_artist (
    artist_key BIGINT IDENTITY(1,1) PRIMARY KEY
    ,artist_id VARCHAR
    ,artist_name VARCHAR(1000)
    ,artist_location VARCHAR(1000)
    ,artist_longitude DOUBLE PRECISION
    ,artist_latitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT PRIMARY KEY
    , time_key_sql TIME
    , hour INT
    , minute INT
    , second INT
    , am_pm VARCHAR(2)
);
""")

date_table_create = ("""
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY
    ,date_key_sql DATE
    ,week INT
    ,month VARCHAR
    ,year INT
    ,is_weekday VARCHAR(8)
);
""")

# STAGING TABLES
staging_events_copy = f"""
COPY staging_song_log
FROM 's3://{MANIFEST_BUCKET}/{MANIFEST_LOG}'
IAM_ROLE '{IAM_ROLE}'
FORMAT AS JSON 's3://{DATA_BUCKET}/{LOG_JSONPATH}'
MANIFEST
REGION '{AWS_REGION}';
"""

#This takes roughly 3.5 hours for the initial data load
staging_songs_copy = (f"""
COPY staging_song_data
FROM 's3://{DATA_BUCKET}/{SONG_DATA}'
IAM_ROLE '{IAM_ROLE}'
FORMAT AS JSON 's3://{MANIFEST_BUCKET}/song_data_paths.json'
REGION '{AWS_REGION}'
MAXERROR 100;
""")


# Use this for any new inserts
# staging_songs_copy = (f"""
# COPY staging_song_data
# FROM 's3://{MANIFEST_BUCKET}/{MANIFEST_SONG}'
# IAM_ROLE '{IAM_ROLE}'
# FORMAT AS JSON 'auto'
# MANIFEST
# REGION '{AWS_REGION}'
# """)

# FINAL TABLES

time_table_copy = (f"""
COPY dim_time
FROM 's3://{MANIFEST_BUCKET}/dim_time.csv'
IAM_ROLE '{IAM_ROLE}'
FORMAT AS CSV 
IGNOREHEADER 1
REGION '{AWS_REGION}';
""")

date_table_copy = (f"""
COPY dim_date
FROM 's3://{MANIFEST_BUCKET}/dim_date.csv'
IAM_ROLE '{IAM_ROLE}'
FORMAT AS CSV
IGNOREHEADER 1
REGION '{AWS_REGION}';
""")

user_table_insert = ("""
INSERT INTO dim_user (user_id, user_first_name, user_last_name, user_gender, user_level)
SELECT DISTINCT
    sl.userId AS user_id
    , sl.firstName AS user_first_name
    , sl.lastName AS user_last_name
    , sl.gender AS user_gender
    , sl.level AS user_level
FROM staging_song_log AS sl
WHERE sl.artist IS NOT NULL -- We don't care about users who don't listen to music;   
""")

artist_table_insert = ("""
INSERT INTO dim_artist (artist_id, 
                        artist_name, 
                        artist_location, 
                        artist_latitude,
                        artist_longitude)
SELECT DISTINCT 
    sd.artist_id AS artist_id
    ,sd.artist_name AS artist_name
    ,sd.artist_location AS artist_location
    ,sd.artist_latitude AS artist_latitude
    ,sd.artist_longitude AS artist_longitude
FROM staging_song_data AS sd
WHERE sd.artist_name IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO dim_song (artist_key,
                      song_id, 
                      artist_id,
                      song_title,
                      song_year, 
                      song_duration)
SELECT DISTINCT
    da.artist_key AS artist_key
    ,sd.song_id AS song_id
    ,sd.artist_id AS artist_id
    ,sd.title AS song_title
    ,sd.year AS song_year
    ,sd.duration AS song_duration
FROM staging_song_data AS sd
LEFT JOIN dim_artist AS da
    ON da.artist_id = sd.artist_id
    AND da.artist_name = sd.artist_name -- Ensures that we get a proper match in case of a new data source
    AND da.artist_location = sd.artist_location --Ensures that we get a proper match in case of a new data source;
WHERE sd.artist_id IS NOT NULL
""")


songplay_table_insert = ("""
INSERT INTO fact_songplay (start_date_key,
                           start_time_key, 
                           user_key, 
                           artist_key, 
                           song_key, 
                           start_datetime,
                           auth, 
                           session_id, 
                           item_in_session, 
                           method, 
                           page, 
                           status, 
                           user_agent, 
                           songplay_location, 
                           registration)
SELECT DISTINCT
    CAST(TO_CHAR(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second', 'YYYYMMDD') AS INTEGER) AS start_date_key
    ,CAST(TO_CHAR(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second', 'HH24MISS') AS INTEGER) AS start_time_key
    ,u.user_key AS user_key
    , a.artist_key AS artist_key
    , s.song_key AS song_key
    , TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_datetime
    , sl.auth AS auth
    , sl.sessionId AS session_id
    , sl.ItemInSession AS item_in_session
    , sl.method AS method
    , sl.page AS page
    , sl.status AS status
    , sl.userAgent AS user_agent
    , sl.location AS songplay_location
    , sl.registration AS registration
FROM staging_song_log sl
LEFT JOIN dim_user AS u
    ON sl.userId = u.user_id
LEFT JOIN dim_artist AS a
    ON sl.artist = a.artist_name
LEFT JOIN dim_song AS s
    ON sl.song = s.song_title 
        AND a.artist_id = s.artist_id
WHERE sl.artist IS NOT NULL
    AND sl.userId IS NOT NULL
    AND a.artist_id IS NOT NULL;
""")

# Truncate tables for debug
truncate_staging_events = "TRUNCATE TABLE staging_song_log;"
truncate_staging_songs = "TRUNCATE TABLE staging_song_data;"

truncate_artist_table = "TRUNCATE TABLE dim_artist;"
truncate_song_table = "TRUNCATE TABLE dim_song;"
truncate_user_table = "TRUNCATE TABLE dim_user;"
truncate_songplay_table = "TRUNCATE TABLE fact_songplay;"
# no trunc are needed for date and time since those are populated on create

# Other useful queries for processing: 
# find out how long on avg it's taking and how many files it did
query_processing_time = """
WITH processing_times AS (
    SELECT
        LAG(curtime) OVER (PARTITION BY query ORDER BY curtime) AS previous_time,
        curtime
    FROM stl_load_commits
    WHERE query = 
)
SELECT
    COUNT(*) AS processed_files,
    AVG(EXTRACT(EPOCH FROM (curtime - previous_time))) AS avg_time_per_file
FROM processing_times
WHERE previous_time IS NOT NULL;
"""



# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create,
                        time_table_create,
                        date_table_create,
                        user_table_create,
                        artist_table_create,
                        song_table_create, 
                        songplay_table_create]

populate_datetime_queries = [date_table_copy, 
                             time_table_copy]

drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop, 
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop, 
                      date_table_drop]

truncate_staging_table_queries = [truncate_staging_events, 
                                  truncate_staging_songs]
truncate_star_table_queries = [truncate_artist_table, 
                               truncate_song_table, 
                               truncate_user_table, 
                               truncate_songplay_table]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [user_table_insert, 
                        artist_table_insert, 
                        song_table_insert,
                        songplay_table_insert]
