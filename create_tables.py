import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# iterate the drop table statements


def drop_tables(cur, conn):
    """
    Droping tables:

    This function is droping the tables if exists

    in order to recreat it to eliminate any errors or to just reset them
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# iterate the create table statements
def create_tables(cur, conn):
    """
    Creating tables:

    This function is creating the tables if they are not exists
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# read the cluster and DB details
def main():
    """
    Load cluster details:

    This function is access the dwh.cfg file in order to read

    the cluster and DB parameters and use them to connect to the cluster.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # execute drop and create table statments
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close the connection
    conn.close()


if __name__ == "__main__":
    main()
