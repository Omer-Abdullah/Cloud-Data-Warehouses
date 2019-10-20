import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# iterate the copy statements
def load_staging_tables(cur, conn):
    """
    Copy datas:
    
    This function is copy the data of the log files of the appliction
    
    which resides in the S3, to the the staging tables.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

# iterate the insert statements
def insert_tables(cur, conn):
    """
    Inserting Data to star schema:
    
    This function is transform and load the data of the staging tables
    
    to the 5 tables of the star schema.
    """
    for query in insert_table_queries:
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
    
    # execute copy and insert statments
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    # close the connection
    conn.close()


if __name__ == "__main__":
    main()