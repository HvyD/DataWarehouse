import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """ Drop all existing tables in sparkify database.
    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables for sparkify database.
    Parameters
    ----------
    cur : cursor
        cursor to the sparkify database connection
    conn : connection
        connection to the sparkify database
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """1. Parses dwh.cfg config file. Please update dwh.cfg to contain
    correct configs AWS cluster host name, database name, database user
    and password and IAM ARN role.
    2. Connects sparkify database on AWS cluster.
    3. Drops existing tables sparkify database and creates new one the tables.
    4. Creates tables for the sparkify database.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()