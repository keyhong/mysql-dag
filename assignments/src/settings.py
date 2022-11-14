import logging
logging.basicConfig(
    format = '%(asctime)s : %(levelname)s : %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO
)

from config_reader import ConfigReader

HOST : str = ConfigReader().get_value('DB_INFO', 'HOST')
PORT : int = int(ConfigReader().get_value('DB_INFO', 'PORT'))
USER : str = ConfigReader().get_value('DB_INFO', 'USER')
PASSWORD: str = ConfigReader().get_value('DB_INFO', 'PASSWORD')
DB: str = ConfigReader().get_value('DB_INFO', 'DB')
CHARSET: str = ConfigReader().get_value('DB_INFO', 'CHARSET')

CHECK_CYCLE: int = int(ConfigReader().get_value('CHECK_OPTION', 'CHECK_CYCLE'))
CHECK_COUNT: int = int(ConfigReader().get_value('CHECK_OPTION', 'CHECK_COUNT'))
