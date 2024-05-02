python-etl-pipeline
======================

파이썬 elt 프로그램

```python
'''
[Question] 문제를 어떻게 해결할 것인가?

* 고려 요소
1. 데이터 사용의 목적 : real-time processing vs batch processing
: 앱을 사용하는 모든 고객 정보(문제#1)과 앱 사용현황 분석(문제#2)에 대한 데이터가 OLAP인가, OLTP인가? ?

(1) OLAP라면, 데이터의 ETL 목적이 분석 또는 통계 또는 ML/DL 등 급한 필요가 아니기 때문에, 업무시간 외에 Batch처리로 한번에 처리하는 것이 좋다.
(2) OLTP라면, 데이터가 실시간으로 조회하고 활용성에 의미가 있다. 이는 실시간 데이터 처리가 가능한 플랫폼 (kafka, spark streaming, storm)등과 함께 쓰일 필요가 있다.

[나의 생각] 고객 정보(문제 #1)를 실시간으로 조회하는 것은 중요하다. 신용과 관련된 조회 및 대출을 통한 수익이 큰 비중을 차지하고 있어,
            필요한 순간 개인의 정보를 조회하는 것은 중요하다. 다만, 대출을 위한 평가가 일정 기간에 걸쳐 진행될 수 있고 내부적으로 언제 평가를 할 것에 대한
            rule이 있다면 은행의 거래를 실시간으로 동기화하는 업무가 보다 더 선순위에 있다고 생각하기 때문에 목적과 리소스의 가용성을 고려해 배치처리를
            해도 무방하지 않을까 싶다. 앱 사용 현황(문제#2) 또한 데이터가 일일 단위가 아닌 일정주기(N주~N달) 간의 사용자에 대한 통계적 분석에 좀 더 유의미해
            보여 서비스 개선에 좀 더 도움이 될 것이라고 판단된다. 따라서 실시간 보다는 일단위의 배치 처리가 좀 더 적합하다고 생각한다.

2. 파이프라인 : 배치처리, 스케줄링, 병렬처리  
: DAG와 같이 비순환그래프의 파이프라인 형태가 필요하다. 또한 특정시간 또는 특정일자에 맞춰 배치처리를 해야하기 때문에
  스케줄링이 필요한데 이에 airflow, azkaban, apach nifi 등 여러 플랫폼이 있다. 해당 플랫폼을 사용한다면 Bash 또는 Python Operator를
  만들어 DAG파일에 정의하고 config에 실행날짜와 시간을 정의하여 해당 문제를 보다 쉽게 해결할 수 있을 것이라 생각한다. 또한
  mysql을 날리는 동안 선행작업에 대해 체크를 해야하는 데, 이를 위해서는 병렬처리가 필요하다. 에를 들어 대표적인 스케줄러 프로그램인 airflow는
  config에 따라 여러 task를 병렬로 처리시킬 수 있고 기본적으로 한 task가 끝나면 다른 task가 대기한다.
  이에 하나는 sql로 쿼리를 실행하는 파일을. 다른 하나는 다른 작업의 수행상태(리눅스 ps 확인하는 cli커맨드)를 쉘로 감싸 BashOperator로 실행시켰을 것 같다.

[Answer] 문제해결의 아이디어 : multi-processing과 Pymysql 이용하기
: 저자는 기존에 있던 airflow(python에서 구동)와 같은 스케줄링 프로그램이 있지만, 기존에 있던 것을 사용하는 것이 아닌 직접 스케줄링 프로그램을
  파이썬으로 만들고자 한다. 이에 다른 스케줄링 프로그램을 참고하여, 해당 플로우를 구동하는 데 

  파이썬이라는 언어의 특성(GIL)에 의해 한 프로세스를 분할하는 multi-threading 는 어려운 것으로 알고있다.(사용할때 성능개선 상황이 명확 : request - callback)
  해당 문제에서는 5분마다 선행작업을 체크하고, 횟수를 60회로 지정하고 있다. 5분 동안 시간을 count한다는 것은 sql쿼리가 실행되는 동안 background에
  또 다른 process가 진행되어야 함을 말한다. 즉, sql쿼리가 시작될 때 두개의 프로세스가 시작되고 프로세스의 존재유무에 따라 PID를 주기적으로 확인하는 방법을 고안하였다.
'''

# [1] TASK를 MySQL과 실행정보를 입력받아 개별 객체마다 excute() 기능을 가진 작업 operator 생성 
# [2] 작업한 Operator들을 담은 Iterable한 자료형을 MySQLDAG에 전달
# [3] DAG는 opertor들의 실행순서를 분석하여, run() 함수를 통해 operator를 실행시키는 반복문을 생성 (실행은 operator의 execute() 함수를 사용)
# [4] DAG의 run() 함수 내의 operator 반복문에서 operator를 인자로 받는 log_loader 컴포넌트를 추가 
#
# ** log_loader의 역할
# (1) operator의 실행 로그 (BATCH_LOG, BATCH_LOG_DETAIL) 기록 및 적재
# (2) 병렬처리(job의 SQL쿼리를 수행할 프로세스와 주기적으로 쿼리 실행 프로세스의 생존을 체크할 프로세스)에서 PID를 받아 5분마다 점검
# (3) SQL쿼리 프로세스가 종료되더라도 5분 체크 주기가 되기 전에 다음 operator 실행을 대기시키기 위해 서브에서 유지하는 역할

# [5] 두 프로세스가 종료되면 실행 로그를 적재하고, 반복문을 이어서 실행(다음 operator 실행)
```

## Directory Summary

| Component                              | Version | Description              |
|----------------------------------------|---------|--------------------------|
| [MySQL](https://www.mysql.com/)        | 8.0.29   | Relational Database     |
| [Python](https://trino.io/)            | 3.7.13  | Program Language         |

### sql

1. Location: python-etl-pipeline/assignments/jobs/
  
### python

1. Location : python-etl-pipeline/assignments/src/
2. Component 
 - entrypoint : python-etl-pipeline/assignments/src/main.py
 - config : python-etl-pipeline/assignments/src/infomation.cfg


## Getting Started

```bash
$ docker compose up -d
```
