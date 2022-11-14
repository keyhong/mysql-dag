#!/usr/bin/env python3
# coding: utf-8

######################################################
#    프로그램명    : mysql_dag.py
#    작성자        : Gyu Won Hong
#    작성일자      : 2022.10.12
#    파라미터      : None
#    설명          : operator의 container를 받아 작업별 태스크를 순서대로 실행해주는 DAG 파일
######################################################

from settings import *
import pymysql
from typing import List, Tuple, Iterable, Type, Any

import os

from datetime import datetime
from mysql_operator import MySQLOperator
from log_loader import LogLoader

import multiprocessing as mp
# mp.set_start_method('spawn')

import json
import time

def record_time_status(pid: int):

    """ settings.py에서 가져온 주기마다 입력값으로 받은 pid의 존재여부를 확인하는 함수

    parameter
    ---------
    pid : int
        병렬처리로 같이 시작하는 다른 프로세스(SQL 쿼리 프로세스)의 PID
    """

    print('p2를 시작합니다')
    cnt = 0

    while cnt <= CHECK_COUNT:
        
        # 자는 시간 : 확인 주기(N) * 60초. 기본적으로 체크주기는 Minute로 하나, 차후 seconds hour에 따라 추가 구현이 필요하다면 가능
        time.sleep(CHECK_CYCLE * 60)
        
        if psutil.pid_exists(pid):
            print(f'{str(datetime.now()).split(".")} 프로세스 P1이 살아있습니다.')
            cnt += 1
            continue
        else:
            print(f'{str(datetime.now()).split(".")} 프로세스 P1이 죽었습니다.')
            return


def excute_query(sql: str, job_id=None, task_id=None):
    logging.info('p1을 시작합니다')

    # mysql 접속 세션 생성
    try:
        mysql_conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB, charset=CHARSET)
    except pymysql.err.OperationalError as err:
        logging.error('MySQL 연결 오류')
        raise err
    else:
        logging.info('MySQL 정상 연결 완료\n')

    with mysql_conn.cursor() as cur:

        try:
            cur.execute(sql)
        except pymysql.err.OperationalError as err:
            logging.error(f'배치 작업 "{job_id} : {task_id}" 실패\nCheck your SQL Query.\n{sql}\n')
        else:
            logging.info(f'배치 작업 "{job_id} : {task_id}" 성공')
            mysql_conn.commit()


class MySQLDAG:

    def __init__(
        self,
        operators: Iterable[Type[MySQLOperator]],
        *args, **kargs):

        # 파이프라인에서 task들의 성공 유무를 판단하기 위한 check_beat. task 실패시에 개발자가 에러를 잡고, run() 함수로 실패지점 부터 다시 작업을 이어나가기 위함.
        self.operators = self._rearrange_order(operators)
        self.op_success: List[int] = [0] * len(self.operators)

    @staticmethod
    def _rearrange_order(operators: Iterable[Type[MySQLOperator]]) -> List[Type[MySQLOperator]]:
        """ 클래스의 인풋 파라미터 중 operators에 대한 실행순서를 다시 재정의 해주는 함수

        Parameters
        ----------
        operators : Iterable[Type[MySQLOperator]]
            인스턴스 생성자 파라미터로 받은 변수. operator들을 담은 iterable한 객체

        Returns
        -------
        operators : List[Type[MySQLOperator]]
            실행 순서를 재배열하여 순서에 따라 operator를 나열한 리스트 객체 

        Notes
        -----
        operator들의 job_id("#O")과task_id("OO_table_OOOOO.sql")를 비교하여 실행 순서를 (Job_id - task_id)별로 재배열한다
        """

        from collections import namedtuple
        SortInfo = namedtuple('SortInfo', ['job_id', 'task_id', 'operator'])

        sort_lst = []

        for operator in operators:
           sort_lst.append(SortInfo(operator.job_id, operator.task_id, operator))
        else:
            sort_lst = sorted(sort_lst, key = lambda x : (x[0], x[1]))
            operators = [ _.operator for _ in sort_lst ]
      
        return operators


    def run(self):
        """ MySQLDAG에 작업 task(MySQLOperator의 인스턴스)들을 담아, 순차적으로 실행하는 함수

        Notes
        ------
        만약 DAG(Direct Acyclic Graph)의 순차적 작업 도중 에러가 발생한다면,
        함수를 재호출 했을 때 이전 실행 실패 지점에서 다시 실행할 수 있도록 구성.

        다만, Airflow처럼 DB 또는 파일에 실행정보에 대한 영구적인 기록을 할 수 있도록 고도화가 필요한 부문.
        현 코딩테스트의 환경에서는 현재는 파이썬 프로세스가 살아있을 때만 가능. 보통 모바일 App이나 Web에서
        Main Process가 죽지않고 계속 살아 있는 경우로 볼 수 있다. 절차적인 main() 또는 cli로 실행하는 경우에는 해당 기능을 사용할 수 없다.
        """

        # 초기 또는 재시작할 task의 작업 순서 번호 ( 0: 실행필요(미실행 또는 이전에 실패), 1: 실행완료(성공) )
        start_idx = self.op_success.index(0)

        # 작업 간의 선/후행 관계에서 선행 작업 실패시 플로우의 중단 여부를 판단할 bool
        IS_ERROR = 0

        # 총 직업 수행 시간을 저장하는 변수
        total_elapsed_time = 0

        # Operator의 실패지점부터 
        for b_i, operator in enumerate(self.operators[start_idx:], start=start_idx):
            
            if IS_ERROR is True:
                break
            
            # LogLoader (배치 작업의 로그 적재 및 ) 인스턴스 생성
            logger = LogLoader(operator, start_idx+b_i)

            logger.set_job_status('RUNNING') 
            logger.set_start_time(datetime.now())
            logger.load_batch_log(total_elapsed_time) # 작업 배치 로그 데이터 적재 (작업종료)
            
            try:
                '''
                [ 기존에 해결하려고 했던 방법 ]

                멀티프로세싱을 이용해 (MySQL쿼리 작업을 수행할 프로세스)와 (쿼리작업의 프로세스가 살아있는 지 PID  일정 주기마다 점검하는 프로세스)를 병렬 처리

                p1, p2 프로세스가 모두 종료 될 때까지 다음 TASK를 실행하지 않고 대기하도록 join()을 걸어 홀딩.
                p1이 우선적으로 종료되고, p2가 p1의 PID가 살아있는 지 확인 후, 종료됨을 확인 후 p2도 정상종료

                ** Flow : (Main Process) -> (쿼리 수행 프로세스 실행) -> (p1의 프로세스를 5분 마다 확인하는 프로세스 실행) -> (p1  종료) -> (p2 종료) -> (Main Process)


                [ 구현 코드]
                p1 = mp.Process(target=operator.execute, args=()) # MySQL 쿼리 작업 프로세스
                p1.start() # 프로세스 p1 시작
                
                p2 = mp.Process(target=logger.record_time_status, args=(p1.pid)) # 일정 시간마다 P1의 PID 존재 여부 점검
                p2.start() # 프로세스 p2 시작

                # 두 SubProcess가 모두 종료될 때까지 대기
                p1.join()
                p2.join()
                
                [ 실패 이유 ]   
                cannot pickle '_io.BufferedReader' object :

                io의 버퍼와 관련된 에러인 것 같은데, 프로세스 객체를 왜 read 할 수 없는 지 조금더 관찰이 필요.
                '''
                db_conn, sql, job_id, task_id = operator.get_exec_info()

                p1 = mp.Process(target=excute_query, args=(sql, job_id, task_id)) # MySQL 쿼리 작업 프로세스
                p1.daemon = True
                p1.start() # 프로세스 p1 시작

                p2 = mp.Process(target=record_time_status, args=(p1.pid, )) # 일정 시간마다 P1의 PID 존재 여부 점검
                p2.daemon = True
                p2.start() # 프로세스 p2 시작

                # 두 SubProcess가 모두 종료될 때까지 대기
                p1.join()
                p2.join()

            except Exception as err:
                # 작업 실패시, IS_ERROR를 TRUE로 설정하여, 반복문(작업 플로우)을 break하여 후행 작업을 못하도록 설정
                IS_ERROR = True
                logger.set_job_status('ERROR') 
                logging.error(f'**배치 작업 "{operator.job_id} {operator.task_id}" 에러 발견\n')

            else:
                # Airflow처럼, 작업이 실패지점부터 재시작하기 위해 작업별 실행성공 유무를 기록하는 과정 (Main Process가 살아있을 때만 가능)
                self.op_success[start_idx + b_i] = 1
                logger.set_job_status('FINISHED') 
                logging.info(f'배치 작업 "{operator.job_id} {operator.task_id}" 정상종료\n')
            
            finally:
                # 작업 종료 시간 기록
                logger.set_end_time(datetime.now()) 

                # DAG에서 작업의 총 수행시간 변수에 해당 작업의 수행시간을 덧셈
                value = logger.get_elapsed_time()
                total_elapsed_time += value

                # 작업 배치 로그 데이터 적재 (작업종료)
                logger.load_batch_log(total_elapsed_time) 

                # 작업 배치 로그 상세 테이블 데이터 적재
                logger.load_batch_log_detail() 

        else:
            # DAG의 모든 작업 수행 후 수행완료 정상 출력문
            logging.info('모든 배치 작업이 끝났습니다')


def __test__():

    # mysql 접속 세션 생성
    try:
        mysql_conn = pymysql.connect(host=HOST, port=PORT, user=USER, password=PASSWORD, db=DB, charset=CHARSET)
    except pymysql.err.OperationalError as err:
        logging.error('MySQL 연결 오류')
        raise err
    else:
        logging.info('MySQL 정상 연결 완료\n')

    # 작업 실행정보 (job_01.json) 로딩
    job1_json = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'jobs', '#1', 'job_01.json')

    with open(job1_json, 'r') as f:
        job_1_info = json.load(f)

    # MySQLOperator 인스턴스 생성
    test_operator1 = MySQLOperator(db_conn=mysql_conn, exec_info=job_1_info[0])
    test_operator2 = MySQLOperator(db_conn=mysql_conn, exec_info=job_1_info[1])
    # test_operator3 = MySQLOperator(db_conn=mysql_conn, exec_info=job_1_info[2])

    # operators = [test_operator1, test_operator2, test_operator3]
    operators = [test_operator1]

    dag = MySQLDAG(operators)
    dag.run()


if __name__ == '__main__':
    __test__()