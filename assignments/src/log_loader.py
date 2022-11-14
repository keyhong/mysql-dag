#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : config_reader.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.11.11
#    파라미터      : None
#    설명          : config 파일을 파싱하여 가져오는 파일 (default : 해당 파일과 동일 경로에 있는 *.cfg를 읽는다)
######################################################

from settings import *

import pymysql
from typing import Type
from datetime import datetime
import psutil
import time

from mysql_operator import MySQLOperator
import os

from settings import *

class LogLoader:
    
    def __init__(self, operator: Type[MySQLOperator], order_number: int):
        
        self.operator = operator
        self.__order_number = order_number
        
        self.__job_id = operator.job_id
        self.__base_day = datetime.now().strftime('%Y%m%d')
        
        self.job_status = 'NULL'
        self.__start_time = 'NULL'
        self.__end_time = 'NULL'


    # 작업상태
    def set_job_status(self, job_status: str):
        if job_status not in ['RUNNING', 'ERROR', 'FINISHED']:
            raise ValueError(f'입력값 {job_status} 에러.')

        self.__job_status = job_status

    # 작업시작일자
    def set_start_time(self, start_time: Type[datetime]):
        self.__start_time = start_time

    def _get_start_time(self) -> str:
        return '"' + str(self.__start_time).split('.')[0] + '"'

    # 작업종료일자
    def set_end_time(self, end_time: Type[datetime]):
        self.__end_time = end_time

    def _get_end_time(self) -> str:
        if self.__end_time == 'NULL':
            return self.__end_time
        else:
            return '"' + str(self.__end_time).split('.')[0] + '"'

    # 작업소요시간
    def get_elapsed_time(self) -> int:
        return (self.__end_time - self.__start_time).seconds


    def load_batch_log(self, total_elapsed_time: Type[datetime]):
        
        db = 'PROJECT'
        target_table = 'BATCH_LOG' 

        sql = f"""
                  INSERT INTO {db}.{target_table} (BASE_DAY, JOB_ID, JOB_STATUS, START_TIME, END_TIME, TOTAL_ELAPSED_TIME)
                  VALUES ( {self.__base_day}, "{self.__job_id}", "{self.__job_status}", {self._get_start_time()}, {self._get_end_time()}, {total_elapsed_time} )
                  ;
               """

        self._excute_query(sql, db, target_table)
        self._execute_analyze_table(db, target_table)

    def load_batch_log_detail(self):
        
        db = 'PROJECT'
        target_table = 'BATCH_LOG_DETAIL'

        sql = f"""
                  INSERT INTO {db}.{target_table} (BASE_DAY, JOB_ID, ORDER_NUMBER, ELAPSED_TIME)
                  VALUES ( {self.__base_day}, "{self.__job_id}", {self.__order_number}, {self.get_elapsed_time()})
                  ;
               """
        self._excute_query(sql, db, target_table)
        self._execute_analyze_table(db, target_table)

    def _excute_query(self, sql: str, db: str, target_table: str):

        with self.operator.db_conn.cursor() as cur:
            
            try:
                cur.execute(sql)
            except pymysql.err.OperationalError as err:
                logging.error(f'Check your SQL Query.\n{sql}')
                raise err
            except pymysql.err.ProgrammingError as err:
                logging.error(f'Check your SQL Query.\n{sql}')
                raise err
            else:
                logging.info(f'배치 작업 "{self.operator.job_id} : {self.operator.task_id}" {db}.{target_table} 적재 완료')
                self.operator.db_conn.commit()

    def _execute_analyze_table(self, db, target_table: str):
        
        with self.operator.db_conn.cursor() as cur:
            
            try:
                cur.execute(f'ANALYZE TABLE {db}.{target_table}')
            except pymysql.err.OperationalError:
                logging.err(f'check your SQL Query.\nANALYZE TABLE {target_table}')
            else:
                logging.info(f'배치 작업 "{self.operator.job_id} : {self.operator.task_id}" ANALYZE TABLE {db}.{target_table} 완료\n')
                self.operator.db_conn.commit()



def __test__():

    logging.info('log_loader.py 테스트 시작\n')

    # mysql 접속 세션을 생성
    try:
        mysql_conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB, charset=CHARSET)
    except pymysql.err.OperationalError as err:
        logging.error('MySQL 연결 오류')
        raise err
    else:
        logging.info('MySQL 정상 연결 완료')

    # 작업 실행정보 (job_01.json) 로딩
    job1_json = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'jobs', '#1', 'job_01.json')

    with open(job1_json, 'r') as f:
        job_1_info = json.load(f)

    # MySQLOperator 인스턴스 생성
    test_operator1 = MySQLOperator(db_conn=mysql_conn, exec_info=job_1_info[0])

    LogLoader(test_operator1, 0)
    logging.info('log_loader.py 테스트 정상 종료')

if __name__ == '__main__':
    import json
    __test__()