"""
Take input of task execution information to execute MySQL tasks
"""

from __future__ import annotations

from typing import Tuple, Type, Dict
import os
import json

import pymysql

from settings import *

class MySQLOperator:
    
    def __init__(
        self,
        db_conn: Type[pymysql.connections.Connection],
        exec_info: Dict[str, str],
        *args, **kargs
    ):
        
        if isinstance(db_conn, pymysql.connections.Connection):
            self.db_conn = db_conn
        else:
            raise TypeError('db_conn should be Type "pymysql.connections.Connection"')
        
        # 작업 ID : '#1', '#2'
        self.job_id = exec_info['job_id'] 

        # 태스크 ID : '01_table_creation.sql', '02_table_insertion.sql' ...
        self.task_id = exec_info['task_id'] 

        # 대상 테이블 : "PROJECT.CST_ININ", "PROJECT.CST_INMN_STT" ...
        self.target_table = exec_info['target_table'] 

        # sql 경로 c:\Users\wnhon\Desktop\t2\project\assignments\jobs\#1\sql\01_table_creation.sql
        self.sql = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'jobs', self.job_id, 'sql', self.task_id)
        self.sql = open(self.sql, encoding='utf-8').read()

    def get_exec_info(self) -> Tuple[str]:
        return (self.db_conn, self.sql, self.job_id, self.task_id,)

    def execute(self):

        with self.db_conn.cursor() as cur:

            try:
                cur.execute(self.sql)
            except pymysql.err.OperationalError as err:
                logging.error(f'배치 작업 "{self.job_id} : {self.task_id}" 실패\nCheck your SQL Query.\n{sql}\n')
            else:
                logging.info(f'배치 작업 "{self.job_id} : {self.task_id}" 성공')
                self.db_conn.commit()

            self._execute_analyze_table()
                
    def _execute_analyze_table(self):
        
        with self.db_conn.cursor() as cur:
            
            try:
                cur.execute(f'ANALYZE TABLE {self.target_table}')
            except pymysql.err.OperationalError:
                logging.err(f'Check your SQL Query.\nANALYZE TABLE {self.target_table}')
            else:
                logging.info(f'ANALYZE TABLE {self.target_table}\n')
                self.db_conn.commit()

    def __del__(self):
        self.db_conn.dispose()


def __test__():

    logging.info('mysql_operator.py 테스트 시작')
    
    # mysql 접속 세션 생성
    try:
        mysql_conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB, charset=CHARSET)
    except pymysql.err.OperationalError as err:
        logging.error('MySQL 연결 오류')
        raise err
    else:
        logging.info('MySQL 정상 연결 완료')

    # JSON 실행정보 가져오기
    job1_json = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'jobs', '#1', 'job_01.json')

    with open(job1_json, 'r') as f:
        job_1_info = json.load(f)
    
    print(job_1_info[0])

    # MySQLOperator 인스턴스 생성
    test_operator = MySQLOperator(db_conn=mysql_conn, exec_info=job_1_info[0])

    # MySQLOperator의 태스크 실행함수인 execute() 테스트
    test_operator.execute()

    logging.info('mysql_operator.py 테스트 정상 종료')

if __name__ == '__main__':

    __test__()