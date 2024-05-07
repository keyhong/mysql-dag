-- 사용자 번호를 기준으로 오름차순 조회한 결과를 파일로 저장
SELECT * 
  FROM PROJECT.CST_ININ
 ORDER BY USR_NO ASC
  INTO OUTFILE '/tmp/result/#1.txt'
;