-- 기준일자, 사용자번호, TOP5메뉴구분을 기준으로 오름차순 조회한 결과를 파일로 저장
SELECT *
  FROM PROJECT.CST_INMN_STT
 ORDER BY STDR_DE ASC, USR_NO ASC, UPP_MENU_5 ASC
  INTO OUTFILE '/tmp/result/#2.txt'