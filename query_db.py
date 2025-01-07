#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 18 15:35:13 2024

@author: greg
"""
import psycopg2
import configparser
import pandas as pd

def query_to_dataframe(query):
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        cur = conn.cursor()
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=columns)
        cur.close()
        conn.close()
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error

def describe_nulls(df):
    print(df.isnull().sum())

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    table_length_queries = ['SELECT COUNT(*) from staging_song_log', 
                            'SELECT COUNT(*) FROM staging_song_data', 
                            'SELECT COUNT(*) FROM dim_user', 
                            'SELECT COUNT(*) FROM dim_artist', 
                            'SELECT COUNT(*) FROM dim_song', 
                            'SELECT COUNT(*) FROM fact_songplay',
                            'SELECT COUNT(*) FROM dim_time', 
                            'SELECT COUNT(*) FROM dim_date']
    
    query_songs_by_day = """
    SELECT dt.am_pm
           ,COUNT(*) AS song_plays
    FROM fact_songplay as fs
    LEFT JOIN dim_time as dt
        on fs.start_time_key = dt.time_key
    GROUP BY am_pm
    ORDER BY am_pm
    """   
    
    query_top_song_weekend = """
    WITH RankedSongs AS (
    SELECT 
        dd.is_weekday, 
        ds.song_title, 
        da.artist_name, 
        count(*) AS songplay_count,
        ROW_NUMBER() OVER (PARTITION BY dd.is_weekday ORDER BY count(*) DESC) AS rank
    FROM fact_songplay AS fs
    LEFT JOIN dim_artist AS da
        ON da.artist_key = fs.artist_key
    LEFT JOIN dim_song AS ds
        ON ds.song_key = fs.song_key
    LEFT JOIN dim_date AS dd
        ON dd.date_key = fs.start_date_key
    GROUP BY dd.is_weekday, ds.song_title, da.artist_name
    )
    SELECT is_weekday, song_title, artist_name, songplay_count
    FROM RankedSongs
    WHERE rank = 1
    ORDER BY is_weekday;
    """
    
    for query in table_length_queries:
        result =  query_to_dataframe(query)
        print(f"Query: {query} -> Count: {result.iloc[0,0]}")
    
    
    
    
    user_dim = query_to_dataframe("SELECT * FROM dim_user")
    artist_dim = query_to_dataframe("SELECT * FROM dim_artist")
    song_dim = query_to_dataframe("SELECT * FROM dim_song")
    
    describe_nulls(user_dim)
    describe_nulls(artist_dim)    
    describe_nulls(song_dim)

    song_log = query_to_dataframe("SELECT * FROM staging_song_log")
    song_fact = query_to_dataframe("SELECT * FROM fact_songplay")
    describe_nulls(song_fact)    
    
    z = query_to_dataframe("SELECT * FROM fact_songplay fs LEFT JOIN staging_song_log AS sl on sl.sessionid = fs.session_id and sl.iteminsession = fs.item_in_session where fs.artist_key IS NULL")
    
    s = query_to_dataframe("SELECT count(*), song_key from fact_songplay group by song_key")
    
    top_weekend = query_to_dataframe(query_top_song_weekend)

if __name__ == "__main__":
    main()
