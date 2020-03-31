import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Loads data from song_data and log_data S3 buckets into staging tables
    in sparkify database.
    Uses copy queries.
    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data from staging tables into fact and dimension tables
    in sparkify database.
    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """1. Parses dwh.cfg config file. dwh.cfg to contain
    correct configs AWS cluster host name, database name, database user
    and passwordand IAM ARN role.
    2. Connects sparkify database on AWS cluster.
    3. Loads data from song_data and log_data S3 buckets into staging tables.
    4. Inserts data from staging tables into fact and dimension tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()