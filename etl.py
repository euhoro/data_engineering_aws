import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    fill_dwh_schema()

def fill_dwh_schema():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn_str = f"""host={config['CLUSTER']['host']} dbname={config['CLUSTER']['db_name']} user={config['CLUSTER']['db_user']} password={config['CLUSTER']['db_password']}  port={config['CLUSTER']['db_port']}"""
    conn = psycopg2.connect(conn_str)
    
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()