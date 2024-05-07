
/* 참고 기존 PROJECT 테이블 

(1) PROJECT.MENU_LOG (메뉴로그)
LOG_TKTM    VARCHAR(14)   NOT NULL  COMMENT '로그일시' @pk (string, 메뉴에 접근한 일시)
LOG_ID      VARCHAR(20)   NOT NULL  COMMENT '로그ID'  @pk (string, 로그를 식별할 수 있는 unique값)
USR_NO      VARCHAR(10)   NULL      COMMENT '사용자번호' (string, 사용자를 식별할 수 있는 값)
MENU_NM     VARCHAR(50)   NULL      COMMENT '메뉴명' (string, 메뉴에 접근한 일시)

-- Q : 사용자 최초가입시, 기본적으로 입력하는 주민등록번호가 변경가능하다?
--     일반적으로는 변경된 주민번호라도, 회원탈퇴 후 재가입을 하는 방향으로.. 진행되는 것이 아닌지..

(2) PROJECT.USR_INFO_CHG_LOG (사용자 정보 변경 로그)
LOG_TKTM    VARCHAR(14)   NOT NULL  COMMENT '로그일시' @pk (string, 메뉴에 접근한 일시)
LOG_ID      VARCHAR(20)   NOT NULL  COMMENT '로그ID' @pk (string, 로그를 식별할 수 있는 unique값)
USR_NO      VARCHAR(10)   NULL      COMMENT '사용자번호'    (string, 사용자를 식별할 수 있는 값)
RSDT_NO     VARCHAR(20)   NULL      COMMENT '주민등록번호'   (string, 고객의 주민번호, 7자리 이후에는 "*" 값으로 표기)
LOC_NM      VARCHAR(10)   NULL      COMMENT '지역명'       (string, 현재 거주중인 지역의 이름)
MCCO_NM     VARCHAR(50)   NULL      COMMENT '이동통신사명'    (string, 현재 가입한 통신사의 이름)

*/ 

-- VIEW 생성
CREATE OR REPLACE VIEW CST_ININ_TMP AS
SELECT 
       USR_NO      VARCHAR(10)   NOT NULL  COMMENT '사용자번호@pk'     -- USER_NUMBER
     , USR_SXDS    VARCHAR(8)    NULL      COMMENT '사용자성별'     -- USER_SEX_DISTINCTION
     , USR_BRT     VARCHAR(10)   NULL      COMMENT '사용자생년월일'  -- USER_BIRTHDAY
     , USR_AGE     INT           NULL      COMMENT '나이'         -- USER_AGE
     , FRST_SBDT   VARCHAR(14)   NULL      COMMENT '최초가입일'      -- FIRST_SUBSCRIBE_DATE, 사용자정보변경로그가 최초로 데이터가 확인되는 시점이 최초
     , MCCO_NM     VARCHAR(50)   NULL      COMMENT '이동통신사명'     -- 기존 테이블 사용
     , LOC_NM      VARCHAR(10)   NULL      COMMENT '지역명'         -- 기존 테이블 사용
     , MO_MENU_NM  VARCHAR(50)   NULL      COMMENT '최빈메뉴명'       -- MODE_MENU
     , PRIMARY KEY (USR_NO)
 FROM PROJECT.MENU_LOG AS A
 LEFT JOIN PROJECT.USR_INFO_CHG_LOG AS B
   ON A.LOG_TKTM = B.LOG_TKTM 
  AND A.LOG_ID = B.LOG_ID





-- 사용자 성별 VIEW
CREATE OR REPLACE VIEW USR_SXDS_TMP AS
SELECT A.*
     , TIMESTAMPDIFF(YEAR, DATE_FORMAT(SUBSTR(A.USR_BRT, 1, 8), '%Y%m%d'), DATE('2022.07.27')) AS USR_AGE
  FROM ( 
    SELECT *
      FROM PROJECT.USR_INFO_CHG_LOG
     WHERE (USR_NO, LOG_TKTM)
        IN (
            SELECT USR_NO, RSDT_NO
                 -- , MAX(LOG_TKTM)
              FROM PROJECT.USR_INFO_CHG_LOG
             WHERE RSDT_NO != ''
             GROUP BY USR_NO
            ) 
       ) A;

-- 사용자생년월일 -> PROJECT.USR_INFO_CHG_LOG (사용자 정보 변경 로그)의 주민등록번호 문자열처리
-- 나이 -> 사용자생년월일을 TIMESTAMPDIFF 함수로 입력 값하고의 차이 계산

SET @STDR_DE := '2022.07.27';

CREATE OR REPLACE VIEW USR_BRT_AGE_TMP AS
SELECT A.*
     , TIMESTAMPDIFF(YEAR, DATE_FORMAT(SUBSTR(A.USR_BRT, 1, 8), '%Y%m%d'), DATE(@STDR_DE)) AS USR_AGE
  FROM ( 
    SELECT USR_NO
         , CONCAT(
                  CASE WHEN SUBSTR(RSDT_NO, 7, 1) in ('0', '9') THEN '18'
                       WHEN SUBSTR(RSDT_NO, 7, 1) in ('1', '2', '5', '6') THEN '19'
                       WHEN SUBSTR(RSDT_NO, 7, 1) in ('3', '4', '7', '8') THEN '20'
                   END
                , SUBSTR(RSDT_NO, 1, 2)
                , SUBSTR(RSDT_NO, 3, 2)
                , SUBSTR(RSDT_NO, 5, 2)
                 ) AS USR_BRT
      FROM PROJECT.USR_INFO_CHG_LOG
     WHERE (USR_NO, LOG_TKTM)
        IN (
            SELECT USR_NO
                 , MAX(LOG_TKTM)
              FROM PROJECT.USR_INFO_CHG_LOG
             WHERE RSDT_NO != ''
             GROUP BY USR_NO
            ) 
       ) A;

-- 최초가입일 -> PROJECT.USR_INFO_CHG_LOG (사용자 정보 변경 로그)의 유저별 최소 로그일시
CREATE OR REPLACE VIEW FRST_SBDT_TMP AS
SELECT USR_NO
     , MIN(LOG_TKTM) AS FRST_SBDT
  FROM PROJECT.USR_INFO_CHG_LOG
 GROUP BY USR_NO


-- 이동통신사명 -> (B.로그 ID, B.로그일시)별 파티셔닝 (orderBy B.로그일시 DESC) -> row_number가 1인 이동통신사명
CREATE OR REPLACE VIEW LOC_NM_TMP AS
SELECT USR_NO
     , MCCO_NM 
  FROM PROJECT.USR_INFO_CHG_LOG
 WHERE (USR_NO, LOG_TKTM)
    IN (
        SELECT USR_NO
             , MAX(LOG_TKTM)
          FROM PROJECT.USR_INFO_CHG_LOG
         WHERE MCCO_NM != ''
         GROUP BY USR_NO
       )

-- 지역명 -> PROJECT.USR_INFO_CHG_LOG (사용자 정보 변경 로그)에서 LOC_NM(지역명)이 NOT_NM
CREATE OR REPLACE VIEW LOC_NM_TMP AS
SELECT USR_NO
     , LOC_NM
  FROM PROJECT.USR_INFO_CHG_LOG
 WHERE (USR_NO, LOG_TKTM)
    IN (
        SELECT USR_NO
             , MAX(LOG_TKTM)
          FROM PROJECT.USR_INFO_CHG_LOG
         WHERE LOC_NM != ''
         GROUP BY USR_NO
       )

-- 최빈메뉴명 -> A.USR_NO, A.MENU_NM 그룹연산
CREATE OR REPLACE VIEW MO_MENU_TMP AS
SELECT B.USR_NO
     , CONCAT(B.MENU_NM, CAST('(건' AS CHAR), ')') AS MO_MENU_NM
  FROM (
        SELECT A.*
             , ROW_NUMBER() OVER (PARTITION BY A.USR_NO ORDER BY A.cnt DESC) AS ranking
          FROM (
                SELECT USR_NO
                     , MENU_NM
                     , COUNT(*) as cnt
                  FROM PROJECT.MENU_LOG
                 GROUP BY  USR_NO, MENU_NM
                HAVING MENU_NM != 'login' 
                   AND MENU_NM != 'logout'
               ) A
        ) B
 WHERE B.ranking = 1;