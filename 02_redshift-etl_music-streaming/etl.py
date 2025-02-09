import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 into staging tables on Redshift.
    
    Executes COPY commands to transfer data from S3 buckets into the staging tables
    (log_data and song_data) on Redshift.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL queries
        conn (psycopg2.connection): Database connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transform and load data from staging tables into analytics tables.
    
    Executes INSERT statements to transform data from staging tables into the 
    analytics tables (fact_songplays and dimension tables) following the star schema.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL queries
        conn (psycopg2.connection): Database connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main ETL function to load and process music streaming data.
    
    1. Establishes connection to the Redshift cluster
    2. Loads raw data from S3 into staging tables
    3. Transforms staging data and loads it into analytics tables
    4. Closes the database connection
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()