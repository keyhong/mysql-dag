/*
1. 프로젝트.고객개인메뉴통계(PROJECT.CUSTOMER_INDIVIDUAL_MENU_STATISTIC) 테이블 생성 
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
*/

-- PROJECT.CST_INMN_STT(고객개인메뉴통계) 
REPLACE INTO PROJECT.CST_INMN_STT
SELECT C.STDR_DE           
     , C.USR_NO
     , C.UPP_MENU_5
 	 , D.CLC_CO_00_06_HR
 	 , D.CLC_CO_06_09_HR
 	 , D.CLC_CO_09_13_HR
 	 , D.CLC_CO_13_16_HR
 	 , D.CLC_CO_16_19_HR
 	 , D.CLC_CO_19_21_HR
 	 , D.CLC_CO_21_24_HR
  FROM (
		SELECT /* 메뉴로그 */
		       B.STDR_DE
			 , B.USR_NO
			 , B.MENU_NM
			 , CONCAT('TOP', B.ranking, '_', B.MENU_NM) AS UPP_MENU_5
		  FROM (
				SELECT A.STDR_DE
					 , A.USR_NO
					 , A.MENU_NM
					 , ROW_NUMBER() OVER (PARTITION BY A.STDR_DE, A.USR_NO ORDER BY A.menu_cnt DESC) as ranking
				  FROM (
						 SELECT SUBSTR(LOG_TKTM, 1, 8) AS STDR_DE
							  , USR_NO
							  , MENU_NM 
							  , COUNT(*) as menu_cnt
						  FROM PROJECT.MENU_LOG M
						 WHERE MENU_NM != 'login'
						   AND MENU_NM != 'logout'
						   AND EXISTS ( 
									   SELECT /* 고객개인정보 */
										      '1'
										 FROM PROJECT.CST_ININ C
										WHERE M.USR_NO = C.USR_NO
										 )
						 GROUP BY SUBSTR(LOG_TKTM, 1, 8), USR_NO, MENU_NM 
					   ) A
				) B
		  WHERE B.ranking <= 5
		) C
    INNER JOIN (
			    SELECT /* 메뉴로그 */
			   	       SUBSTR(LOG_TKTM, 1, 8) AS STDR_DE
			   	     , USR_NO
			   	     , MENU_NM
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('00', '01', '02', '03', '04', '05') THEN 1 ELSE 0 END) AS 'CLC_CO_00_06_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('06', '07', '08') THEN 1 ELSE 0 END) AS 'CLC_CO_06_09_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('09', '10', '11') THEN 1 ELSE 0 END) AS 'CLC_CO_09_13_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('12', '13', '14') THEN 1 ELSE 0 END) AS 'CLC_CO_13_16_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('15', '16', '17') THEN 1 ELSE 0 END) AS 'CLC_CO_16_19_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('18', '19', '20') THEN 1 ELSE 0 END) AS 'CLC_CO_19_21_HR'
			   	     , SUM(CASE WHEN SUBSTR(LOG_TKTM, 9, 2) IN ('21', '22', '23') THEN 1 ELSE 0 END) AS 'CLC_CO_21_24_HR'
			      FROM PROJECT.MENU_LOG
			     WHERE MENU_NM != 'login'
			       AND MENU_NM != 'logout'
			     GROUP BY SUBSTR(LOG_TKTM, 1, 8), USR_NO, MENU_NM
			   ) D
      ON C.STDR_DE = D.STDR_DE AND C.USR_NO = D.USR_NO AND C.MENU_NM = D.MENU_NM