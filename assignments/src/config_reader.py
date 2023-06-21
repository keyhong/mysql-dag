#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : config_reader.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.10.10
#    파라미터      : None
#    설명          : config 파일을 파싱하여 가져오는 파일 (default : 해당 파일과 동일 경로에 있는 *.cfg를 읽는다)
######################################################

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
'''
실제 App 구현을 한다면 Query를 날리는 등 개별 사용자가 각 사용자 config에 맞게 여러 화면(개별 class)에서
공통된 설정 값을 가져오기 위해 ConfigReader를 싱글톤 패턴 적용하였다. 이는 앱 사용동안 단 하나의 인스턴스의
메모리 주소에 접근하여 여러 인스턴스가 생성되는 것을 막아 메모리 누수를 막을 수 있다.

ConfigReader는 사용자별 CONFIG에 맞게 설정을 영구저장하는 파일이다. 물론, 실제 앱 구현을 한다면 사용자로 부터
인풋 값을 받아 유효성을 검정하고 유효성이 인정될 때만 DB로 read와 write를 할 수 있는 등. 일부 section의 값에는
설정 변경에 대한 허가여부 및 값 범위에 대한 검정이 필요하겠지만, 해당 환경에서는 MySQL의 DB접속 정보 및
그 외 부가적인 정보 Log Level, Job의 분기 디렉토리 경로 등을 명시적으로 기재하였다. 
'''

class ConfigReader:

    def __new__(cls, CFG_DIR: str=None):

        # config 위치 경로에 대한 인풋값 설정이 없다면, 현재 파일 실행 디렉토리에 config 파일이 있다고 가정
        if CFG_DIR is None: 
            cls.CFG_DIR = os.path.dirname(os.path.abspath(__file__))         
        # class의 input 파라미터에 대한 타입 검정        
        elif isinstance(CFG_DIR, str):
            raise TypeError('input var CFG_DIR should be str.')
        # config 위치 경로에 대한 인풋값 설정이 잘못됬다면(폴더가 없는 경우)에도 현재 파일 실행 디렉토리로 config 파일 위치 폴더로 지정
        elif not isdir(CFG_DIR): 
            logging.error(f'input variable CFG_DIR "{CFG_DIR}"  is not exists. Set base CFG_DIR "{os.path.dirname(os.path.abspath(__file__))}"')
            cls.CFG_DIR = os.path.dirname(os.path.abspath(__file__)) 

        # glob 라이브러리를 이용해 폴더에 있는 .cfg 파일의 절대경로를 파악. cfg 파일은 하나만 있어야 하므로, assert를 통해 값을 검정
        cfg_file: List[str] = glob.glob(f'{os.path.join(cls.CFG_DIR, "*.cfg")}')
        assert len(cfg_file) == 1, f'*.cfg file should be only one in CFG_DIR "{cls.CFG_DIR}"'

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


def __test__():

    logging.info('log_loader.py 테스트 시작\n')

    # cfg파일 중 일부 추출
    '''
    [CHECK_OPTION]
    CHECK_CYCLE=5
    CHECK_COUNT=60
    '''

    # reader 인스턴스 생성
    cfg_reader = ConfigReader()

    # 정상 데이터 기재 체큰
    section_name = 'CHECK_OPTION'
    logging.info(f'section : "{section_name}"을 확인합니다.')

    option_name = 'CHECK_CYCLE'
    logging.info(f'request_option : "{option_name}"을 확인합니다.')

    response = cfg_reader.get_value(section_name, option_name)
    logging.info(f'respose : "{response}"을 정상적으로 받아 왔습니다.\n')


    # 에러 데이터 기재 예외처리(section이 없는 경우)
    try:
        section_name = 'NO_Section'
        option_name = 'CHECK_CYCLE'
        response = cfg_reader.get_value(section_name, option_name)
    except configparser.NoSectionError: 
        logging.error(f'section : "{section_name}"는 없습니다.\n')


    # 에러 데이터 기재 예외처리(section이 없는 경우)
    try:
        section_name = 'CHECK_OPTION'
        option_name = 'NO_Option'
    except configparser.NoOptionErrorr:
        logging.error(f'option : "{option_name}"는 없습니다.\n')

    logging.info('log_loader.py 테스트 종료')


if __name__ == '__main__':
    __test__()
   
