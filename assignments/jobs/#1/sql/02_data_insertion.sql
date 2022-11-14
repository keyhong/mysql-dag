REPLACE INTO PROJECT.CST_ININ
SELECT IF(G.USR_NO           = '' OR G.USR_NO       IS NULL, '-' , G.USR_NO) AS USR_NO
     , IF(G.USR_SXDS         = '' OR G.USR_SXDS     IS NULL, '-' , G.USR_SXDS) AS USR_SXDS
     , IF(G.USR_BRT          = '' OR G.USR_BRT      IS NULL, '-' , G.USR_BRT) AS USR_BRT
     , IF(G.USR_AGE          = '' OR G.USR_AGE      IS NULL, '-' , G.USR_AGE) AS USR_AGE
     , IF(G.FRST_SBDT        = '' OR G.FRST_SBDT    IS NULL, '-' , G.FRST_SBDT) AS FRST_SBDT
     , IF(G.MCCO_NM          = '' OR G.MCCO_NM      IS NULL, '-' , G.MCCO_NM) AS MCCO_NM
     , IF(G.LOC_NM           = '' OR G.LOC_NM       IS NULL, '-' , G.LOC_NM) AS LOC_NM
     , IF(G.MO_MENU_NM       = '' OR G.MO_MENU_NM   IS NULL, '-' , G.MO_MENU_NM) AS MO_MENU_NM
FROM (
	SELECT A.USR_NO
		 , IFNULL(B.USR_SXDS, '-') AS USR_SXDS
		 , IFNULL(C.USR_BRT, '-') AS USR_BRT
		 , IFNULL(C.USR_AGE, '-') AS USR_AGE
		 , IFNULL(D.FRST_SBDT, '-') AS FRST_SBDT
		 , IFNULL(F.MCCO_NM, '-') AS MCCO_NM
		 , IFNULL(G.LOC_NM, '-') AS LOC_NM
		 , IFNULL(H.MO_MENU_NM, '-') AS MO_MENU_NM 
	  FROM (
			SELECT DISTINCT USR_NO
			  FROM PROJECT.MENU_LOG
			 ORDER BY USR_NO ASC
		   ) A 
	  LEFT JOIN (
				 SELECT USR_NO,
						CASE WHEN SUBSTR(RSDT_NO, 7, 1) IN ('1', '3', '5', '7') THEN 'M'
							 WHEN SUBSTR(RSDT_NO, 7, 1) IN ('2', '4', '6', '8') THEN 'F'
				   ELSE '-'
					END AS USR_SXDS
				   FROM PROJECT.USR_INFO_CHG_LOG
				  WHERE (USR_NO, LOG_TKTM)
					 IN (
						 SELECT USR_NO
							  , MAX(LOG_TKTM)
						   FROM PROJECT.USR_INFO_CHG_LOG
						  WHERE RSDT_NO != ''
					   GROUP BY USR_NO
						)
				) B 
			 ON A.USR_NO = B.USR_NO
	  LEFT JOIN (
				 SELECT A.USR_NO
					  , A.USR_BRT
					  , CAST(TIMESTAMPDIFF(YEAR, DATE_FORMAT(SUBSTR(A.USR_BRT, 1, 8), '%Y%m%d'), DATE('2022-07-27')) AS CHAR) AS USR_AGE
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
					   ) A
				) C
			 ON A.USR_NO = C.USR_NO
	  LEFT JOIN (
				 SELECT USR_NO
					  , SUBSTR(MIN(LOG_TKTM), 1, 8) AS FRST_SBDT
				   FROM PROJECT.USR_INFO_CHG_LOG
				  GROUP BY USR_NO
				) D
			 ON A.USR_NO = D.USR_NO
	  LEFT JOIN (
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
				 ) F
			 ON A.USR_NO = F.USR_NO
	  LEFT JOIN (
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
				) G
			 ON A.USR_NO = G.USR_NO
	  LEFT JOIN (		 
				 SELECT B.USR_NO
					  , CONCAT(B.MENU_NM, '(', B.cnt, '건)') AS MO_MENU_NM
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
				  WHERE B.ranking = 1
				) H
			 ON A.USR_NO = H.USR_NO
	) G
;