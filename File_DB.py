import pandas as pd
import configparser
config = configparser.ConfigParser()
# read the configuration file
config.read('my_config.ini')


def flat_file(c):
    File_path = config.get(c, 'file_path')
    File_name = config.get(c, 'file_name')
    Delimiter = config.get(c, 'delimiter')
    Extension = config.get(c, 'file_extension')
    Sheet_name = config.get(c, 'sheet_name')
    Full_path = File_path+'\\'+File_name
    if Extension.lower() == 'csv' or Extension.lower() == 'txt' or Extension.lower() == 'tsv':
        file = pd.read_csv(Full_path, delimiter= Delimiter)
        # file = file.astype(str)

    elif Extension.lower() == 'xlsx':
        file = pd.read_excel(Full_path, sheet_name= Sheet_name)
        # file = file.astype(str)


    elif Extension.lower() == 'json':
        file = pd.read_json(Full_path)
        # file = file.astype(str)

    elif Extension.lower() == 'parquet':
        file = pd.read_parquet(Full_path)
        # file = file.astype(str)

    return file