"""Module import."""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Fetch data from s3 bucket & dump into staging table."""
    for query in copy_table_queries:
        print('Loading staging tables...')
        cur.execute(query)
        conn.commit()
        print('Staging tables loaded')


def insert_tables(cur, conn):
    """Query and pouplate fact and dimession tables."""
    for query in insert_table_queries:
        print('Loading fact and dimenssion tables...')
        cur.execute(query)
        conn.commit()
        print('Fact and dimenssion tables loaded')


def main():
    """Establish database connection and perform ELT(extract load and transform)."""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(f"host={config['CLUSTER']['HOST']} \
                            dbname={config['CLUSTER']['DB_NAME']} user={config['CLUSTER']['DB_USER']} \
             password={config['CLUSTER']['DB_PASSWORD']} port={config['CLUSTER']['DB_PORT']}")
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
