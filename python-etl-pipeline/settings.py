import os
import logging
logging.basicConfig(
    format = '%(asctime)s : %(levelname)s : %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO
)

from config_reader import ConfigReader

HOST = os.getenv("HOST", ConfigReader.get_value('DB_INFO', 'HOST'))
PORT = os.getenv("HOST", int(ConfigReader.get_value('DB_INFO', 'PORT')))
USER = os.getenv("USER", ConfigReader.get_value('DB_INFO', 'USER'))
PASSWORD = os.getenv("PASSWORD", ConfigReader.get_value('DB_INFO', 'PASSWORD'))
DB = os.getenv("DB", ConfigReader.get_value('DB_INFO', 'DB'))
CHARSET = os.getenv("CHARSET", ConfigReader.get_value('DB_INFO', 'CHARSET'))

CHECK_CYCLE = os.getenv("CHECK_CYCLE", int(ConfigReader.get_value('CHECK_OPTION', 'CHECK_CYCLE')))
CHECK_COUNT = os.getenv("CHECK_COUNT", int(ConfigReader.get_value('CHECK_OPTION', 'CHECK_COUNT')))
