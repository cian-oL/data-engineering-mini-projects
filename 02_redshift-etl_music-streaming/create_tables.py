import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all existing tables in the Redshift database.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL queries
        conn (psycopg2.connection): Database connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging and analytics tables in the Redshift database.
    
    Args:
        cur (psycopg2.cursor): Cursor object for executing SQL queries
        conn (psycopg2.connection): Database connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function to reset and create tables in Redshift.
    
    1. Establishes connection to the Redshift cluster
    2. Drops all existing tables
    3. Creates new staging and analytics tables
    4. Closes the connection
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config["CLUSTER"].values()
        )
    )
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
