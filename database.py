import configparser
import psycopg2
from psycopg2 import Error
import cx_Oracle
config = configparser.ConfigParser()
# read the configuration file
config.read('my_config.ini')
import snowflake.connector as sc




        
def postgre(c):
    conn = psycopg2.connect(user=config.get(c, 'username'),
                                    password=config.get(c, 'password'),
                                    host=config.get(c, 'host'),
                                    port="5432")

    return conn

def snowflake(c):
    conn = sc.connect(account=config.get(c, 'host'),
                                user=config.get(c, 'user'),
                                password=config.get(c, 'password'))
    return conn
def oracle(c):
    dsn_tns = cx_Oracle.makedsn(host=config.get(c, 'host'), port="1521",service_name=config.get(c,'service_name'))
    conn = cx_Oracle.connect(user=config.get(c,'username'),
                                password=config.get(c, 'password'), dsn=dsn_tns)
              
    return conn

def source_connection(source_conn):
    dbname=config.get(source_conn, 'name')
    if dbname.lower() == 'oracle':
        ret_source_conn = oracle(source_conn)
    elif dbname.lower() == 'postgre':
        ret_source_conn = postgre(source_conn)
    elif dbname.lower() == 'snowflake':
        ret_source_conn = snowflake(source_conn)
    else:
        ret_source_conn = None
    return ret_source_conn


def target_connection(target_conn):
    dbname=config.get(target_conn, 'name')
    if dbname.lower() == 'oracle':
        ret_target_conn = oracle(target_conn)
    elif dbname.lower() == 'postgre':
        ret_target_conn = postgre(target_conn)
    elif dbname.lower() == 'snowflake':
        ret_target_conn = snowflake(target_conn)
    else:
        ret_target_conn = None
    return ret_target_conn