"""Module import."""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Dropes SQL tables one by one."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Create SQL tables one by one."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Establish database connection and Create redshift tables one by one."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        f"host={config['CLUSTER']['HOST']} dbname={config['CLUSTER']['DB_NAME']} \
              user={config['CLUSTER']['DB_USER']} \
             password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
