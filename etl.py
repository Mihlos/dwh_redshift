import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Insert the data from our Bucket
    to the staging tables.
    
    Parameters:
        cur  (object): connection cursor.
        conn (object): connection to our redshift db.
        
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert the data from our staging
    tables to the final tables.
    
    Parameters:
        cur  (object): connection cursor.
        conn (object): connection to our redshift db.
        
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Read the configuration from dwh.cfg
    
    Create the conection to the Redshift Cluster.
    
    Call the functions to populate staging and final.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()