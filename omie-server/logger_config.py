import os
import sys
import logging
from logging.handlers import RotatingFileHandler

LOG_PATH = '.'
def setBasePath(path):
    global LOG_PATH
    LOG_PATH = os.path.join(path, 'Logs')

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Set up a RotatingFileHandler to log messages to a file
    os.makedirs(LOG_PATH, exist_ok=True)
    log_file = os.path.join(LOG_PATH, 'app.log')    
    
    max_log_file_size = 10 * 1024 * 1024  # 10 MB
    max_log_file_count = 5

    file_handler = RotatingFileHandler(log_file, maxBytes=max_log_file_size, backupCount=max_log_file_count)
    file_handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    # Set up a StreamHandler to log messages to STDOUT
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)

    logger.addHandler(stream_handler)

    return logger
