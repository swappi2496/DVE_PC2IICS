import configparser
import pandas as pd
import psycopg2
from psycopg2 import Error

config = configparser.ConfigParser()
# read the configuration file
config.read('my_config.ini')
# get all the connections
hostname = config.get('c01', 'host')
username = config.get('c01', 'username')
password = config.get('c01', 'password')


conn = psycopg2.connect(user=username,
                              password=password,
                              host=hostname,
                              port="5432")
df1 = pd.read_sql_query('select * from sample',conn)
print(df1.head())