import configparser
config = configparser.ConfigParser()
# connection to the database
config['c02'] = {'Name' : 'Snowflake',
                'code':'snowflake',
                'username': 'devsingh01',
                'password': 'Qwerty123',
                'host': 'je93546.switzerland-north.azure'}
# write the 
with open('PC_IICS/my_config.ini', 'a') as configfile:
    config.write(configfile)