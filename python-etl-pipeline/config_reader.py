# coding: utf-8

from __future__ import annotations

import glob
import configparser
from typing import List
import os
import logging
logging.basicConfig(
    format = '%(asctime)s : %(levelname)s : %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    level = logging.INFO
)

"""
실제 App 구현을 한다면 Query를 날리는 등 개별 사용자가 각 사용자 config에 맞게 여러 화면(개별 class)에서
공통된 설정 값을 가져오기 위해 ConfigReader를 싱글톤 패턴 적용하였다. 이는 앱 사용동안 단 하나의 인스턴스의
메모리 주소에 접근하여 여러 인스턴스가 생성되는 것을 막아 메모리 누수를 막을 수 있다.

ConfigReader는 사용자별 CONFIG에 맞게 설정을 영구저장하는 파일이다. 물론, 실제 앱 구현을 한다면 사용자로 부터
인풋 값을 받아 유효성을 검정하고 유효성이 인정될 때만 DB로 read와 write를 할 수 있는 등. 일부 section의 값에는
설정 변경에 대한 허가여부 및 값 범위에 대한 검정이 필요하겠지만, 해당 환경에서는 MySQL의 DB접속 정보 및
그 외 부가적인 정보 Log Level, Job의 분기 디렉토리 경로 등을 명시적으로 기재하였다. 
"""

class ConfigReader:

    def __new__(cls, config_dir: str=None):

        # config 위치 경로에 대한 인풋값 설정이 없다면, 현재 파일 실행 디렉토리에 config 파일이 있다고 가정
        if config_dir is None: 
            cls.config_dir = os.path.dirname(os.path.abspath(__file__))         
        # class의 input 파라미터에 대한 타입 검정        
        elif isinstance(config_dir, str):
            raise TypeError('input var config_dir should be str.')
        # config 위치 경로에 대한 인풋값 설정이 잘못됬다면(폴더가 없는 경우)에도 현재 파일 실행 디렉토리로 config 파일 위치 폴더로 지정
        elif not os.isdir(config_dir): 
            logging.error(f'input variable config_dir "{config_dir}"  is not exists. Set base config_dir "{os.path.dirname(os.path.abspath(__file__))}"')
            cls.config_dir = os.path.dirname(os.path.abspath(__file__)) 

        # glob 라이브러리를 이용해 폴더에 있는 .cfg 파일의 절대경로를 파악. cfg 파일은 하나만 있어야 하므로, assert를 통해 값을 검정
        cfg_file: List[str] = glob.glob(f'{os.path.join(cls.config_dir, "*.cfg")}')
        assert len(cfg_file) == 1, f'*.cfg file should be only one in config_dir "{cls.config_dir}"'

        # 정상 실행시, configparser 라이브리의 ConfigParser 인스턴스를 만들어 파일 읽기
        cls.cfg_reader = configparser.ConfigParser()
        cls.cfg_reader.read(cfg_file)

        obj = super().__new__(cls)
        return cls

    @classmethod
    def get_value(cls, section:str, option: str) -> str:

        try:
            return_value = cls.cfg_reader.get(section, option)

        # Section이 없는 경우
        except configparser.NoSectionError as err: 
            logging.error(f'Input Parameter section "{section}" is not exist.')
            raise err

        # option 없는 경우
        except configparser.NoOptionError as err:
            logging.error(f'Input Parameter Section "{option}" is not exist.')
            raise err
        
        return return_value