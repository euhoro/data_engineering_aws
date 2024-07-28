import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Drop all tables in the Redshift database.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing PostgreSQL commands.
        conn (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create all tables in the Redshift database.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing PostgreSQL commands.
        conn (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_dwh_schema():
    """Create the data warehouse schema in Redshift.

    This function connects to the Redshift cluster, drops existing tables, and creates new tables as defined in the SQL queries.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_str = f"""host={config['CLUSTER']['host']} dbname={config['CLUSTER']['db_name']} user={config['CLUSTER']['db_user']} password={config['CLUSTER']['db_password']}  port={config['CLUSTER']['db_port']}"""
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    create_dwh_schema()