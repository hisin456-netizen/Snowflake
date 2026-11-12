-- ======================================================================
-- ALL_TAB_COMMENTS 메타 파일 적재 (익명 블록 형태의 개발/테스트 버전)
--   스테이지 파일 -> S0(ALL_TAB_COMMENTS_S) -> RAW(ALL_TAB_COMMENTS)
-- EG_SQ.sql(프로시저 버전)과 다른 점:
--   1) 프로시저가 아니라 DECLARE/BEGIN/END 익명 블록이라 그대로 실행해서 시험한다 (P_BAT_ID 는 변수로 둠)
--   2) S0 에 SN_ETL_RN 을 붙여 같은 기준일/OWNER/TABLE_NAME 중복 중 최신 파일 1건만 남긴다
--   3) 마지막에 오래된 RAW 데이터를 삭제(PRUNING)하는 단계가 있다
--   4) S0 를 시작 전이 아니라 적재 후에 비운다
-- ======================================================================
DECLARE
    P_BAT_ID                 VARCHAR; --추후 인자로..

    V_SP_NAME                VARCHAR;
    V_DATABASE_NAME          VARCHAR DEFAULT 'DW';
    V_SCHEMA_NAME            VARCHAR DEFAULT 'DW_META';
    V_TABLE_NAME             VARCHAR DEFAULT 'ALL_TAB_COMMENTS';
    V_EXEC_STEP              VARCHAR DEFAULT 'STEP.0:INIT';

    V_EXEC_ID                VARCHAR; --실행 ID
    V_PROC_TYPE              VARCHAR DEFAULT 'PERIOD'; -- FULL/PERIOD/MERGE
    V_PARAMS                 VARIANT;

    V_COPY_QUERY_ID          VARCHAR;
    V_COPY_RET               VARIANT;
    V_COPY_FLAG              NUMBER(38,0) DEFAULT 0;
    V_COPY_LOADED_CNT        NUMBER(38,0); --FILE COUNT
    V_COPY_ROWS_PARSED       NUMBER(38,0); --ROW COUNT

    V_STATUS                 VARCHAR; -- RUNNING / SUCCESS / FAILED / SKIPPED
    V_RAW_DELETE_CNT         NUMBER(38,0);
    V_RAW_INSERT_CNT         NUMBER(38,0);
    V_RAW_MERGE_INSERT_CNT   NUMBER(38,0);
    V_RAW_MERGE_UPDATE_CNT   NUMBER(38,0);
    V_RAW_MERGE_DELETE_CNT   NUMBER(38,0);
    V_RAW_PRUNING_DELETE_CNT NUMBER(38,0);
    V_REMARK                 VARCHAR;
    V_LOG_RES                VARCHAR;
BEGIN
    /*
    ##. 처리개요
    DATA FILE 적재 방식 : FULL 또는 PERIOD
    RAW 적재 방식 : REPLACE 또는 MERGE

    ##. 흐름
    DATA FILE 적재 방식 : PERIOD
    RAW 적재 방식 : REPLACE
    #. S0 : DELETE
    #. S0 : COPY INTO
    #. S0 : IF SUMMARY BASE DATE
    #. RAW : REPLACE - DELETE / INSERT
    #. RAW : DATA PRUNING - OLD DATA

    S0 : DW.DW_META.ALL_TAB_COMMENTS_S
    RAW : DW.DW_META.ALL_TAB_COMMENTS
    */
    --STEP.0:INIT
    -- 실행 ID 를 만들고, 테스트 편의상 배치 ID 도 같은 값으로 쓴다 (프로시저 버전에서는 인자로 받음)
    V_EXEC_ID := DW.DW_META.FN_GEN_EXEC_ID();
    P_BAT_ID := V_EXEC_ID; --임시설정
    V_SP_NAME := DW.DW_META.FN_GET_SP_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME);
    V_PARAMS := OBJECT_CONSTRUCT('P_BAT_ID', :P_BAT_ID);
    --EG. V_PARAMS := OBJECT_CONSTRUCT('S_DT', :V_S_DT, 'E_DT', :V_E_DT);
    
    --LOG INIT
    -- 실행 로그 시작 행 INSERT (RUNNING). SP_DW_SP_LOG_I 는 UTILS.sql 에서 주석 처리되어 있어 계정에 없으면 여기서 실패한다
    CALL DW.DW_META.SP_DW_SP_LOG_I(:V_EXEC_ID, :P_BAT_ID, :V_SP_NAME, :V_PROC_TYPE, :V_PARAMS, :V_EXEC_STEP) INTO :V_LOG_RES;

	-- --SN_ETL_FILE_CONTENT_KEY VARCHAR(134217728),
	-- SN_ETL_FILE_LAST_MODIFIED TIMESTAMP_NTZ,
	-- --SN_ETL_START_SCAN_TIME TIMESTAMP_LTZ,
	-- SN_ETL_EXT_BASE_DT VARCHAR(134217728),
 --    SN_ETL_PROC_DTTM TIMESTAMP_LTZ,
 --    SN_ETL_RN NUMBER(38, 0)
    -- 스테이지 폴더(날짜 하위 폴더 포함)의 모든 파일을 S0 로 COPY
    --   $1:컬럼 : JSON 키를 컬럼 타입으로 변환,  METADATA$* : Snowflake 가 주는 파일 정보(파일명, 행 번호, 수정 시각)
    --   SN_ETL_EXT_BASE_DT : 파일 경로 'oracle/meta/<테이블>/<날짜>/파일' 의 4번째 조각 = 날짜 폴더
    --   SN_ETL_PROC_DTTM   : 적재 시각
    --   SN_ETL_RN          : 같은 기준일/OWNER/TABLE_NAME 안에서 파일 수정 시각 최신순 번호 (1 = 최신)
    V_EXEC_STEP := 'STEP.X:COPY INTO TO STAGE TABLE';
    COPY INTO DW.DW_META.ALL_TAB_COMMENTS_S (OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT, SN_ETL_PROC_DTTM, SN_ETL_RN)
    FROM (
        SELECT $1:OWNER::TEXT, $1:TABLE_NAME::TEXT, $1:TABLE_TYPE::TEXT, $1:COMMENTS::TEXT, $1:ORIGIN_CON_ID::NUMBER(38, 0)
             , METADATA$FILENAME AS SN_ETL_FILENAME
             , METADATA$FILE_ROW_NUMBER AS SN_ETL_FILE_ROW_NUMBER
             --, METADATA$FILE_CONTENT_KEY AS SN_ETL_FILE_CONTENT_KEY
             , METADATA$FILE_LAST_MODIFIED AS SN_ETL_FILE_LAST_MODIFIED
             --, METADATA$START_SCAN_TIME AS SN_ETL_START_SCAN_TIME
             , SPLIT_PART(SN_ETL_FILENAME, '/', 4) AS SN_ETL_EXT_BASE_DT
             , current_timestamp() AS SN_ETL_PROC_DTTM
             , row_number() over (partition by SN_ETL_EXT_BASE_DT, $1:OWNER::TEXT, $1:TABLE_NAME::TEXT order by SN_ETL_FILE_LAST_MODIFIED desc) AS SN_ETL_RN             
          FROM @DW.DW_SCH.STG/oracle/meta/all_tab_comments/ (file_format => 'DW.DW_SCH.FF_JSON_STRIP')
    );
    --STEP.X:COPY RESULT SCAN
    -- 방금 COPY 의 결과를 읽어 FN_GET_COPY_RESULT_T 로 요약한다 (V_COPY_FLAG: 0 정상 / 1 실패 / 2 처리할 파일 없음)
    V_EXEC_STEP := 'STEP.X:COPY RESULT SCAN';
    V_COPY_QUERY_ID := LAST_QUERY_ID();
    --EG. SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(:V_COPY_QUERY_ID));
    SELECT RET, LOADED_CNT, ROWS_PARSED INTO :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON(:V_COPY_RET)));

    -- COPY 가 정상(0)일 때만 RAW 를 갱신한다. 그 외는 아래 ELSE 에서 비고만 남긴다
    IF (V_COPY_FLAG = 0) THEN
        -- 기준일별 건수를 처리 이력 테이블에 기록 (ROW_COUNT: 전체 행 수, RN_ROW_COUNT: 중복 제거 후 남을 행 수)
        V_EXEC_STEP := 'STEP.X:GET BASE DATE FROM STAGE TABLE TO DW.DW_META.DW_PROC_PERIOD';
        INSERT INTO DW.DW_META.DW_PROC_PERIOD
        SELECT CURRENT_TIMESTAMP() AS PROC_DT, :V_SP_NAME AS SP_NAME, DW.DW_META.FN_GET_S0_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS SO_TBL_NAME, DW.DW_META.FN_GET_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS TBL_NAME, SN_ETL_EXT_BASE_DT
             , COUNT(*) AS ROW_COUNT, SUM(CASE WHEN SN_ETL_RN = 1 THEN SN_ETL_RN ELSE NULL END) AS RN_ROW_COUNT
          FROM DW.DW_META.ALL_TAB_COMMENTS_S
         GROUP BY SN_ETL_EXT_BASE_DT;

        -- 중복 제거: 같은 키가 여러 파일에 있으면 최신 파일(SN_ETL_RN=1)만 남기고 나머지 삭제
        V_EXEC_STEP := 'STEP.X:Cleansing STAGE TABLE';
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS_S WHERE SN_ETL_RN > 1;
    
        -- RAW 교체 1단계: S0 에 들어온 기준일과 같은 RAW 행을 삭제
        V_EXEC_STEP := 'STEP.X:DELETE RAW USING STAGE TABLE.SN_ETL_EXT_BASE_DT';
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT IN 
        (SELECT SN_ETL_EXT_BASE_DT FROM DW.DW_META.ALL_TAB_COMMENTS_S GROUP BY SN_ETL_EXT_BASE_DT);
        V_RAW_DELETE_CNT := SQLROWCOUNT;

        --INSERT RAW
        -- RAW 교체 2단계: S0 를 RAW 에 입력 (SN_ETL_RN 은 RAW 에 없어 제외하고 컬럼 순서를 맞춘다)
        V_EXEC_STEP := 'STEP.X:INSERT RAW USING STAGE TABLE';
        INSERT INTO DW.DW_META.ALL_TAB_COMMENTS
        SELECT OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT, SN_ETL_PROC_DTTM
          FROM DW.DW_META.ALL_TAB_COMMENTS_S;
        V_RAW_INSERT_CNT := SQLROWCOUNT;

        -- 적재가 끝난 S0 비우기 (중간에 실패하면 이 단계에 못 가서 S0 에 찌꺼기가 남는다)
        V_EXEC_STEP := 'STEP.X:DELETE STAGE TABLE';
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS_S;
    ELSE
        V_EXEC_STEP := 'STEP.X:NO COPY RESULT';
        V_REMARK := 'NO COPY FILES';
    END IF;

    -- 오래된 데이터 정리: 기준일이 '20250101' 이하인 RAW 행 삭제 (기준일은 코드에 고정된 값)
    -- COPY 결과와 상관없이 IF 블록 밖에서 항상 실행된다
    V_EXEC_STEP := 'DATA PRUNING - OLD DATA';
    --@@@ SN_ETL_EXT_BASE_DT : DATA TYPE => VARCHAR, DATE, TIMESTAMP AND YYYYMMDD, YYYYMM, YYYY
    DELETE FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT <= '20250101';
    --SELECT * FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT <= '20250101';
    V_RAW_PRUNING_DELETE_CNT := SQLROWCOUNT;
    
    -- 정상 종료: 로그 행을 SUCCESS 와 건수로 갱신 (오류 인자 3개는 NULL)
    V_EXEC_STEP := 'STEP.X:FINISH.SUCCESS';
    V_STATUS := 'SUCCESS';
    CALL DW.DW_META.SP_DW_SP_LOG_U(:V_EXEC_ID, :V_EXEC_STEP, :V_STATUS, :V_COPY_QUERY_ID
                                 , :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED, :V_RAW_DELETE_CNT, :V_RAW_INSERT_CNT
                                 , NULL, :V_RAW_MERGE_INSERT_CNT, :V_RAW_MERGE_UPDATE_CNT, :V_RAW_MERGE_DELETE_CNT, :V_RAW_PRUNING_DELETE_CNT
                                 , NULL, NULL, NULL, :V_REMARK) INTO :V_LOG_RES;
    RETURN :V_STATUS;
EXCEPTION
    -- 어느 단계에서든 오류가 나면: 실패한 단계(V_EXEC_STEP)와 오류 코드/상태/메시지를 로그에 남기고 FAILED 반환
    WHEN OTHER THEN
        V_STATUS := 'FAILED';
        CALL DW.DW_META.SP_DW_SP_LOG_U(:V_EXEC_ID, :V_EXEC_STEP, :V_STATUS, :V_COPY_QUERY_ID
                                     , :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED, :V_RAW_DELETE_CNT, :V_RAW_INSERT_CNT
                                     , NULL, :V_RAW_MERGE_INSERT_CNT, :V_RAW_MERGE_UPDATE_CNT, :V_RAW_MERGE_DELETE_CNT, :V_RAW_PRUNING_DELETE_CNT
                                     , :SQLCODE, :SQLSTATE, :SQLERRM, :V_REMARK) INTO :V_LOG_RES;
        RETURN :V_STATUS || ' @' || :V_EXEC_STEP || ' : ' || :SQLERRM;
END;

-- ----------------------------------------------------------------------
-- 아래 /* ... */ 와 이어지는 주석 영역은 개발 중 쓰던 메모/테스트 쿼리 모음이다 (실행되지 않음)
--   - 프로시저 호출 예, 처리 이력/로그/S0/RAW 조회, COPY 결과 요약 함수 시험(정상 8파일, 파일 없음 케이스)
--   - 20261027 등 예전 날짜 경로와 특정 쿼리 ID 는 그 시점의 값이라 지금은 맞지 않는다
-- ----------------------------------------------------------------------
/*
DECLARE
    V_SP_RES VARCHAR;
BEGIN
    CALL DW.DW_META.SP_DW_DW_META_ALL_TAB_COMMENTS('TEST_BAT_ID') INTO :V_SP_RES;
    RETURN :V_SP_RES;
END;

CREATE TRANSIENT TABLE DW.DW_META.DW_PROC_PERIOD AS 
SELECT CURRENT_DATE() AS PROC_DT, 'SP_DW_DW_META_ALL_TAB_COMMENTS' AS SP_NAME, 'DW.DW_META.ALL_TAB_COMMENTS_S' AS SO_TBL_NAME, 'DW.DW_META.ALL_TAB_COMMENTS' AS TBL_NAME, SN_ETL_EXT_BASE_DT, COUNT(*) AS ROW_COUNT FROM DW.DW_META.ALL_TAB_COMMENTS_S GROUP BY SN_ETL_EXT_BASE_DT;

SELECT * FROM DW.DW_META.DW_SP_LOG ORDER BY 1 DESC;

    V_COPY_QUERY_ID := LAST_QUERY_ID();
    --EG. SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(:V_COPY_QUERY_ID));
    SELECT RET, LOADED_CNT, ROWS_PARSED INTO :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON(:V_COPY_RET)));
    


select * FROM TABLE(RESULT_SCAN('01c8128c-0002-470d-0001-abae001963ba'));
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(RESULT_SCAN('01c8128c-0002-470d-0001-abae001963ba'));

SELECT * FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON('
[
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027100338_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027155712_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027144446_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261023/all_tab_comments_20261023155610_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027100144_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027153403_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027095835_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  },
  {
    "error_limit": 1,
    "errors_seen": 0,
    "file": "stg/oracle/meta/all_tab_comments/20261029/all_tab_comments_20261029131844_00000001.json.gz",
    "rows_loaded": 17,
    "rows_parsed": 17,
    "status": "LOADED"
  }
]
')));


select * FROM TABLE(RESULT_SCAN('01c838f8-0002-514c-0001-abae002106a6')); --NODATA
SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE(RESULT_SCAN('01c838f8-0002-514c-0001-abae002106a6')); --NODATA

SELECT * FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON('[
  {
    "status": "Copy executed with 0 files processed."
  }
]')));

SELECT * FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(SELECT PARSE_JSON(ARRAY_AGG(OBJECT_CONSTRUCT(*))) FROM TABLE(RESULT_SCAN('01c838f8-0002-514c-0001-abae002106a6'))
));

SELECT * FROM DW.DW_META.DW_PROC_PERIOD;

SELECT * FROM DW.DW_META.ALL_TAB_COMMENTS;

SELECT * FROM DW.DW_META.ALL_TAB_COMMENTS_S ORDER BY SN_ETL_EXT_BASE_DT;

TRUNCATE TABLE DW.DW_META.ALL_TAB_COMMENTS_S;


        SELECT CURRENT_TIMESTAMP() AS PROC_DT, SN_ETL_EXT_BASE_DT, COUNT(*) AS ROW_COUNT, SUM(CASE WHEN SN_ETL_RN = 1 THEN SN_ETL_RN ELSE NULL END) AS RN_ROW_COUNT
          FROM DW.DW_META.ALL_TAB_COMMENTS_S
         GROUP BY SN_ETL_EXT_BASE_DT;
         

SELECT *
, DW.DW_META.FN_GET_COPY_RESULT(PARSE_JSON(SO_TBL_NAME))
FROM DW.DW_META.DW_PROC_PERIOD WHERE SO_TBL_NAME LIKE '%status%';

SELECT DW.DW_META.FN_GET_COPY_RESULT(TO_VARIANT([{"status":"Copy executed with 0 files processed."}]));

SELECT PARSE_JSON('[{"status":"Copy executed with 0 files processed."}]')
, DW.DW_META.FN_GET_COPY_RESULT(PARSE_JSON('[{"status":"Copy executed with 0 files processed."}]'))
;

WITH BASE AS (
    SELECT DW.DW_META.FN_GET_COPY_RESULT(PARSE_JSON('[{"status":"Copy executed with 0 files processed."}]'))
)
SELECT * FROM BASE
*/

-- SELECT * FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON('[{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261029/all_tab_comments_20261029131844_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027153403_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027100338_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027155712_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027144446_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027095835_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261023/all_tab_comments_20261023155610_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"},{"error_limit":1,"errors_seen":0,"file":"stg/oracle/meta/all_tab_comments/20261027/all_tab_comments_20261027100144_00000001.json.gz","rows_loaded":17,"rows_parsed":17,"status":"LOADED"}]')));



-- EXEC_ID, BAT_ID, SP_NAME, PROC_TYPE, PARAMS, EXEC_STEP, STATUS, START_TS, END_TS, ELAPSED_SEC, COPY_QUERY_ID, COPY_FLAG, COPY_FILE_CNT, COPY_LOADED_CNT, RAW_DELETE_CNT, RAW_INSERT_CNT, RAW_MERGE_INSERT_CNT, RAW_MERGE_UPDATE_CNT, RAW_MERGE_DELETE_CNT, RAW_PRUNING_DELETE_CNT, ERROR_CODE, ERROR_STATE, ERROR_MSG, WAREHOUSE_NAME, REMARK

-- INSERT INTO DW.DW_META.DW_SP_LOG(EXEC_ID, BAT_ID, SP_NAME, PROC_TYPE, PARAMS, EXEC_STEP, STATUS, START_TS, WAREHOUSE_NAME)
-- SELECT :P_EXEC_ID, :P_BAT_ID, :P_SP_NAME, :P_PROC_TYPE, :P_PARAMS, :P_EXEC_STEP, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_WAREHOUSE()
-- -- SELECT :P_EXEC_ID, :P_BAT_ID, :P_SP_NAME, :P_PROC_TYPE, :P_PARAMS, :P_EXEC_STEP, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_WAREHOUSE()
-- ;

/*
        SELECT $1:OWNER::TEXT, $1:TABLE_NAME::TEXT, $1:TABLE_TYPE::TEXT, $1:COMMENTS::TEXT, $1:ORIGIN_CON_ID::NUMBER(38, 0)
             , METADATA$FILENAME AS SN_ETL_FILENAME
             , METADATA$FILE_ROW_NUMBER AS SN_ETL_FILE_ROW_NUMBER
             , METADATA$FILE_CONTENT_KEY AS SN_ETL_FILE_CONTENT_KEY
             , METADATA$FILE_LAST_MODIFIED AS SN_ETL_FILE_LAST_MODIFIED
             , METADATA$START_SCAN_TIME AS SN_ETL_START_SCAN_TIME
             , SPLIT_PART(SN_ETL_FILENAME, '/', 4) AS SN_ETL_EXT_BASE_DT
             , current_timestamp() AS SN_ETL_PROC_DTTM
             , row_number() over (partition by SN_ETL_EXT_BASE_DT, $1:OWNER::TEXT, $1:TABLE_NAME::TEXT order by SN_ETL_FILE_LAST_MODIFIED desc) AS RN
          FROM @DW.DW_SCH.STG/oracle/meta/all_tab_comments/ (file_format => 'DW.DW_SCH.FF_JSON_STRIP')
          ;
*/

//SQL compilation error: Invalid identifier INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER

-- CALL DW.DW_META.SP_DW_SP_LOG_I(:V_EXEC_ID, :P_BAT_ID, :V_SP_NAME, :V_PROC_TYPE, :V_PARAMS, :V_EXEC_STEP);
//SQL compilation error:Unknown user-defined function DW.DW_META.SP_DW_SP_LOG_I.

//SQL compilation error: Invalid identifier INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER


-- FAILED @STEP.X:COPY INTO TO STAGE TABLE : SQL compilation error:
-- Table 'DW.DW_META.ALL_TAB_COMMENTS_S' does not exist