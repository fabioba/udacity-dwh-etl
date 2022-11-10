"""
This module includes all the queries necessary to populate those tables

Author: Fabio Barbazza
Date: Nov, 2022
"""
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import logging

logger = logging.getLogger(__name__)

def load_staging_tables(cur, conn):
    """
        This method is responsible for loading staging table

        Args:
            cur(cursor)
            conn(connection)
    """
    try:

        for query in copy_table_queries:
            logger.info('query :{}'.format(query))
            cur.execute(query)
            conn.commit()

        logger.info('load staging')

    except Exception as err:
        logger.exception(err)
        raise err

def insert_tables(cur, conn):
    """
        This method is responsible for inserting data

        Args:
            cur(cursor)
            conn(connection)
    """
    try:

        for query in insert_table_queries:
            logger.info('query :{}'.format(query))

            cur.execute(query)
            conn.commit()

        logger.info('insert tables')

    except Exception as err:
        logger.exception(err)
        raise err

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()