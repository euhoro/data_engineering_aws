import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, check_duplicates_queries


def load_staging_tables(cur, conn):
    """Load data from S3 into staging tables in Redshift.
    
    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing PostgreSQL commands.
        conn (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert data from staging tables into final tables in Redshift.
    
    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing PostgreSQL commands.
        conn (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def check_duplicates(cur, conn):
    """Check for duplicate entries in the final tables.
    
    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing PostgreSQL commands.
        conn (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
    """
    for query in check_duplicates_queries:
        cur.execute(query)
        results = cur.fetchall()
        if results:
            print(f"Duplicates found: {results}")
        else:
            print("No duplicates found.")

def fill_dwh_schema():
    """Load data from S3 into staging tables, insert into final tables, and check for duplicates.
    Reads the configuration from 'dwh.cfg' to establish the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_str = f"""host={config['CLUSTER']['host']} dbname={config['CLUSTER']['db_name']} user={config['CLUSTER']['db_user']} password={config['CLUSTER']['db_password']}  port={config['CLUSTER']['db_port']}"""
    conn = psycopg2.connect(conn_str)
    
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    check_duplicates(cur, conn)

    conn.close()


if __name__ == "__main__":
    fill_dwh_schema()