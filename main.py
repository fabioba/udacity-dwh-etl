"""
This module is the entrypoint of the project

Author: Fabio Barbazza
Date: Nov, 2022
"""
import configparser
import psycopg2
import logging

import create_tables
import etl

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=logging.INFO)

logger = logging.getLogger(__name__)




def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print(config['IAM_ROLE']['ARN'])

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    create_tables.drop_tables(cur, conn)
    create_tables.create_tables(cur, conn)
    
    etl.load_staging_tables(cur, conn)
    etl.insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()