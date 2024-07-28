import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # host = aaa
    # db_name = dwh
    # db_user = a
    # db_password = p
    # db_port = 5439
    conn_str = f"""host={config['CLUSTER']['host']} dbname={config['CLUSTER']['db_name']} user={config['CLUSTER']['db_user']} password={config['CLUSTER']['db_password']}  port={config['CLUSTER']['db_port']}"""
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()