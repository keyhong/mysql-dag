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

-- 1. 프로젝트.고객개인메뉴통계(PROJECT.CUSTOMER_INDIVIDUAL_MENU_STATISTIC) 테이블 생성 
CREATE TABLE IF NOT EXISTS PROJECT.CST_INMN_STT (
       STDR_DE            VARCHAR(8)    NOT NULL  COMMENT '기준일자@pk'       -- STANDARD_DATE
     , USR_NO             VARCHAR(10)   NOT NULL  COMMENT '사용자번호@pk'       -- USER_NUMBER
     , UPP_MENU_5         VARCHAR(50)   NOT NULL  COMMENT 'TOP5메뉴구분@pk'       -- USER_MENU_FIVE, PROJECT.MENU_LOG MODE_MENU 스키마 참고
     , CLC_CO_00_06_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_00_06_시간'     -- CLICK_COUNT_00_06_HOUR
     , CLC_CO_06_09_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_06_09_시간'     -- CLICK_COUNT_06_09_HOUR
     , CLC_CO_09_13_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_09_12_시간'     -- CLICK_COUNT_09_13_HOUR
     , CLC_CO_13_16_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_12_15_시간'     -- CLICK_COUNT_13_16_HOUR
     , CLC_CO_16_19_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_15_18_시간'     -- CLICK_COUNT_16_19_HOUR
     , CLC_CO_19_21_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_18_21_시간'     -- CLICK_COUNT_19_21_HOUR	 
     , CLC_CO_21_24_HR    INT UNSIGNED  NOT NULL  COMMENT '클릭건수_21_24_시간'     -- CLICK_COUNT_21_24_HOUR	 
     , PRIMARY KEY (STDR_DE, USR_NO, UPP_MENU_5) 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT '고객개인메뉴통계'
;