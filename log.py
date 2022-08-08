import logging
from datetime import datetime
import os
LOG_FILENAME = datetime.now().strftime('logfile_%d-%m-%Y_%H_%M_%S.log')

def get_logger(name):
    if not os.path.exists('logs'):
        os.makedirs('logs')
    else:
        pass
    log_format = '%(asctime)s  %(message)s'
    logging.basicConfig(level=logging.INFO,
                        format=log_format,
                        filename=f'logs/{LOG_FILENAME}',
                        filemode='w')

    return logging.getLogger(name)