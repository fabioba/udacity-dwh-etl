"""
This module includes all the steps necessary to create tables

Author: Fabio Barbazza
Date: Nov, 2022
"""
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging

logger = logging.getLogger(__name__)


def drop_tables(cur, conn):
    """
        This method is responsible for dropping table

        Args:
            cur(cursor)
            conn(connection)
    """
    try:
            
        for query in drop_table_queries:

            logger.info('run: {}'.format(query))

            cur.execute(query)
            conn.commit()

        logger.info('success drop tables')

    except Exception as err:
        logger.exception(err)
        raise err


def create_tables(cur, conn):
    """
        This method is responsible for creating table

        Args:
            cur(cursor)
            conn(connection)
    """
    try:
        for query in create_table_queries:
            logger.info('run: {}'.format(query))

            cur.execute(query)
            conn.commit()
        logger.info('success create tables')

    except Exception as err:
        logger.exception(err)
        raise err

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()