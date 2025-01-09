"""
Module: query_db.py.

This module contains utility functions and queries for inspecting data in a
Redshift data warehouse. It provides functionality to execute SQL queries,
convert query results into a Pandas DataFrame, and analyze null values in
tables. The script includes predefined queries for analyzing song plays and
top songs during weekends.

Functions:
----------
- query_to_dataframe(query): Executes a SQL query on Redshift and returns the
  results as a Pandas DataFrame.
- describe_nulls(df): Displays the count of null values in each column of a
  DataFrame.
- main(): Executes predefined queries to inspect data and outputs results.

Dependencies:
-------------
- psycopg2: For connecting to and executing queries on the Redshift database.
- configparser: For reading Redshift configuration from `dwh.cfg`.
- pandas: For handling query results and performing data analysis.

Execution:
----------
This script is executed as a standalone program. Ensure the `dwh.cfg` file is
configured with the appropriate AWS and Redshift credentials and settings.

To run the script:
    $ python query_db.py
"""

import psycopg2
import configparser
import pandas as pd

config = configparser.ConfigParser()
config.read('dwh.cfg')


def query_to_dataframe(query):
    """
    Execute a SQL query on Redshift and return the results as a Pandas DF.

    This function connects to a Redshift database, executes the provided query,
    retrieves the results, and converts them into a Pandas DataFrame. If an
    error occurs, it prints the error and returns an empty DataFrame.

    Args:
    -----
    query : str
        The SQL query to execute.

    Returns:
    --------
    pandas.DataFrame
        A DataFrame containing the query results. If an error occurs, an empty
        DataFrame is returned.
    """
    try:
        conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                .format(*config['CLUSTER']
                                        .values()))
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
    """
    Display the count of null values in each column of a DataFrame.

    This function checks for null values in each column of the provided
    DataFrame and prints the count of nulls for each column.

    Args:
    -----
    df : pandas.DataFrame
        The DataFrame to analyze for null values.

    Returns:
    --------
    None
        This function prints the null value counts and does not return any
        values.
    """
    print(df.isnull().sum())


def main():
    """
    Inspect data in the Redshift data warehouse using predefined queries.

    This function performs the following steps:
    1. Reads Redshift configuration values from `dwh.cfg`.
    2. Executes a series of predefined queries to:
        - Count rows in various tables.
        - Analyze song plays by time of day.
        - Identify the most-played song during weekends.
    3. Outputs query results to the console.

    Predefined Queries:
    --------------------
    - `table_length_queries`: Counts rows in staging and dimension tables.
    - `query_songs_by_day`: Analyzes song plays by AM/PM.
    - `query_top_song_weekend`: Identifies the top-played song on weekends.

    Args:
    -----
    None

    Returns:
    --------
    None
        This function executes predefined queries and outputs results.
    """
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
        result = query_to_dataframe(query)
        print(f"Query: {query} -> Count: {result.iloc[0, 0]}")

    print(query_to_dataframe(query=query_songs_by_day))
    print(query_to_dataframe(query=query_top_song_weekend))


if __name__ == "__main__":
    """
    Entry point for executing the data inspection script.

    This block ensures that the `main` function is executed only when the
    script is run directly, not when it is imported as a module in another
    script.
    """
    main()
