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

-- 1. 프로젝트.고객개인정보(PROJECT.CUSTOMER_INDIVIDUAL_INFORMATION) 테이블 생성 
CREATE TABLE IF NOT EXISTS PROJECT.CST_ININ (
       USR_NO      VARCHAR(10)   NOT NULL  COMMENT '사용자번호'       -- USER_NUMBER
     , USR_SXDS    VARCHAR(8)    NULL      COMMENT '사용자성별'       -- USER_SEX_DISTINCTION
     , USR_BRT     VARCHAR(10)   NULL      COMMENT '사용자생년월일'    -- USER_BIRTHDAY
     , USR_AGE     VARCHAR(4)    NULL      COMMENT '나이'          -- USER_AGE
     , FRST_SBDT   VARCHAR(14)   NULL      COMMENT '최초가입일'      -- FIRST_SUBSCRIBE_DATE, 사용자정보변경로그가 최초로 데이터가 확인되는 시점이 최초
     , MCCO_NM     VARCHAR(50)   NULL      COMMENT '이동통신사명'     -- PROJECT.USR_INFO_CHG_LOG MCCO_NM 스키마 참고
     , LOC_NM      VARCHAR(10)   NULL      COMMENT '지역명'         -- PROJECT.USR_INFO_CHG_LOG MODE_MENU 스키마 참고
     , MO_MENU_NM  VARCHAR(50)   NULL      COMMENT '최빈메뉴명'       -- PROJECT.MENU_LOG MODE_MENU 스키마 참고
     , PRIMARY KEY (USR_NO)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT '고객개인정보'
;
