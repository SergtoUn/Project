import configparser
import psycopg2
from sql_queries import copy_table_queries, staging_data_quality_checks, insert_table_queries, insert_data_quality_check
import logging
import log_setup
from collections import Counter

'''
def put_players(cur, conn):
    cur.execute(insert_players)
    conn.commit()
'''



def load_staging_tables(cur, conn):
    '''
        Description: This function can be used to load the staging tables from S3.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    logging.info('Loading staging tables...')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def staging_quality_checks(cur, conn):
    '''
        Description: This function can be used to check if the staging tables are loaded correctly from S3.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    try:
        logging.info('Data quality checks of the staging tables...')
        dq_results = list()
        i = 0
        for query in staging_data_quality_checks:
            cur.execute(query)
            conn.commit()
            dq_results.insert(i, cur.fetchall())
            i = i + 1
        
        false_count = Counter(dq_results)[False]
        false_indices = [n for n in range(len(dq_results)) if not dq_results[n]]
    
        
        if false_count > 0:
            logging.error("Data quality checks of the staging tables failed")
            logging.warning("The following tests were not passed:")
            for ind in false_indices:
                logging.warning(staging_data_quality_checks[ind])

        else:
            logging.info("Data quality checks for staging tables are finished successfully")
            
    except Exception as e :
        logging.exception(e.msg)
        
def insert_tables(cur, conn):
    '''
        Description: This function can be used to insert the data into the production tables of the warehouse from the staging tables.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    logging.info('Inserting data into DWH tables...')
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def insert_quality_check(cur, conn):
    '''
        Description: This function can be used to insert the data into the production tables of the warehouse from the staging tables.
        
        Arguments:
            cur: cursor
            conn: connection to the database
            
        Returns:
            None
    '''
    try:
        cur.execute(insert_data_quality_check)
        conn.commit()
        result = cur.fetchall()
        if result != 'PASSED':
            logging.ERROR(result)
        else:
            logging.info(result)

    except Exception as e:
        logging.exception(e.msg)

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    staging_quality_checks(cur, conn)
    insert_tables(cur, conn)

    conn.close()
 

if __name__ == "__main__":
    main()