-- ======================================================================
-- DW_META 공통 유틸: 이름 생성 함수, 적재 결과 해석 함수, 실행 이력 테이블/프로시저
-- 구성: 1) 이름 생성 함수  2) COPY/MERGE 결과 해석 함수  3) 실행 ID 생성
--       4) 처리 이력 테이블(DW_PROC_PERIOD, DW_SP_LOG)  5) 로그 갱신 프로시저
-- ======================================================================

-- ===== 1. 이름 생성 함수 =====
-- 프로시저 이름 생성: (DB, 스키마, 테이블) -> 'SP_DB_SCHEMA_TABLE' (공백 제거, 대문자)
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_SP_NAME(
    P_DATABASE_NAME STRING,
    P_SCHEMA_NAME   STRING,
    P_TABLE_NAME    STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
    'SP_' || UPPER(TRIM(P_DATABASE_NAME)) || '_' || UPPER(TRIM(P_SCHEMA_NAME))   || '_' || UPPER(TRIM(P_TABLE_NAME))
$$;

-- 테이블 전체 이름 생성: (DB, 스키마, 테이블) -> 'DB.SCHEMA.TABLE' (공백 제거, 대문자)
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_TABLE_NAME(
    P_DATABASE_NAME STRING,
    P_SCHEMA_NAME   STRING,
    P_TABLE_NAME    STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
    UPPER(TRIM(P_DATABASE_NAME)) || '.' || UPPER(TRIM(P_SCHEMA_NAME))   || '.' || UPPER(TRIM(P_TABLE_NAME))
$$;

-- S0(원본 적재) 테이블 이름 생성: FN_GET_TABLE_NAME 결과 + '_S' -> 'DB.SCHEMA.TABLE_S'
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_S0_TABLE_NAME(
    P_DATABASE_NAME STRING,
    P_SCHEMA_NAME   STRING,
    P_TABLE_NAME    STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
    DW.DW_META.FN_GET_TABLE_NAME(P_DATABASE_NAME, P_SCHEMA_NAME,P_TABLE_NAME) || '_S'
$$;

-- ===== 2. COPY / MERGE 결과 해석 함수 =====
-- 아래 주석 처리된 SP_GET_COPY_RESULT, SP_GET_COPY_RESULT1, FN_GET_COPY_RESULT 는
-- COPY INTO 결과를 읽던 이전 시도로, 현재는 쓰지 않고 FN_GET_COPY_RESULT_T 로 대체되었다.
--미사용
-- CREATE OR REPLACE PROCEDURE DW.DW_META.SP_GET_COPY_RESULT(P_QID STRING)
-- RETURNS VARIANT
-- LANGUAGE SQL
-- AS
-- $$
-- DECLARE
--     V_RET VARIANT;
-- BEGIN
--     WITH SRC AS (
--         SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS J FROM TABLE(RESULT_SCAN(:P_QID))
--     ), BASE AS (
-- SELECT
--     f.INDEX                                   AS SEQ,
--     f.VALUE:file::STRING                      AS FILE_NAME,
--     f.VALUE:status::STRING                    AS STATUS,
--     f.VALUE:rows_parsed::NUMBER               AS ROWS_PARSED,
--     f.VALUE:rows_loaded::NUMBER               AS ROWS_LOADED,
--     f.VALUE:error_limit::NUMBER               AS ERROR_LIMIT,
--     f.VALUE:errors_seen::NUMBER               AS ERRORS_SEEN
-- FROM SRC,
--      LATERAL FLATTEN(INPUT => SRC.J) f
-- ), BASE_AGG AS (
--     SELECT COUNT(*) AS TOT_CNT
--     , SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) AS LOADED_CNT
--     , SUM(ROWS_PARSED) AS ROWS_PARSED
--     , SUM(ROWS_LOADED) AS ROWS_LOADED
--     , SUM(ERROR_LIMIT) AS ERROR_LIMIT
--     , SUM(ERRORS_SEEN) AS ERRORS_SEEN
--     , CASE WHEN TOT_CNT = LOADED_CNT THEN 1 ELSE 0 END AS RET
--      FROM BASE
-- )
-- SELECT ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(*)) AS RET
-- INTO :V_RET
-- FROM BASE_AGG;

-- RETURN V_RET;
-- END;
-- $$;

-- --미사용
-- CREATE OR REPLACE PROCEDURE DW.DW_META.SP_GET_COPY_RESULT1(P_QID STRING)
-- RETURNS VARIANT
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     V_RET VARIANT;
-- BEGIN
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS J
--     INTO :V_RET
--     FROM TABLE(RESULT_SCAN(:P_QID));
    
--     RETURN V_RET;
-- END;
-- $$;


-- CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_COPY_RESULT(
--     P_QID STRING
-- )
-- RETURNS VARIANT
-- LANGUAGE SQL
-- AS
-- $$
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) AS J
--     FROM TABLE(RESULT_SCAN(P_QID))
-- $$;

--미사용
-- CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_COPY_RESULT(P_JS VARIANT)
-- RETURNS VARIANT
-- LANGUAGE SQL
-- AS
-- $$
--     WITH SRC AS (
--         SELECT P_JS AS J
--     ), BASE AS (
--         SELECT
--             f.INDEX                     AS SEQ,
--             f.VALUE:file::STRING        AS FILE_NAME,
--             f.VALUE:status::STRING      AS STATUS,
--             f.VALUE:rows_parsed::NUMBER AS ROWS_PARSED,
--             f.VALUE:rows_loaded::NUMBER AS ROWS_LOADED,
--             f.VALUE:error_limit::NUMBER AS ERROR_LIMIT,
--             f.VALUE:errors_seen::NUMBER AS ERRORS_SEEN
--         FROM SRC,
--              LATERAL FLATTEN(INPUT => SRC.J) f
--     ), BASE_AGG AS (
--         SELECT COUNT(*)                                    AS TOT_CNT,
--                COUNT_IF(STATUS = 'LOADED')                 AS LOADED_CNT,
--                COALESCE(SUM(ROWS_PARSED), 0)               AS ROWS_PARSED,
--                COALESCE(SUM(ROWS_LOADED), 0)               AS ROWS_LOADED,
--                COALESCE(SUM(ERROR_LIMIT), 0)               AS ERROR_LIMIT,
--                COALESCE(SUM(ERRORS_SEEN), 0)               AS ERRORS_SEEN
--         FROM BASE
--     ), BASE_RET AS (
--         SELECT A.*,
--                IFF(TOT_CNT > 0 AND TOT_CNT = LOADED_CNT AND ERRORS_SEEN = 0, 1, 0) AS RET
--         FROM BASE_AGG A
--     )
--     SELECT ARRAY_AGG(OBJECT_CONSTRUCT_KEEP_NULL(*))::VARIANT AS RET
--     FROM BASE_RET
-- $$;

--사용
-- COPY INTO 결과(파일별 결과 JSON 배열)를 받아 합계 1행으로 요약하는 테이블 함수
--   입력 P_JS : COPY INTO 결과 행들을 배열로 모은 VARIANT (파일마다 file/status/rows_parsed/rows_loaded/error_limit/errors_seen)
--   TOT_CNT     : 결과 행(파일) 수
--   LOADED_CNT  : status 가 LOADED 인 파일 수
--   ROWS_PARSED / ROWS_LOADED : 읽은 행 수 / 적재된 행 수 (합계)
--   ERROR_LIMIT / ERRORS_SEEN : 오류 허용 한도 / 발견된 오류 수 (합계)
--   RET         : 0 정상, 2 처리할 데이터 없음(LOADED 0건, 오류 없음), 1 실패
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_COPY_RESULT_T(P_JS VARIANT)
RETURNS TABLE (
    TOT_CNT     NUMBER,
    LOADED_CNT  NUMBER,
    ROWS_PARSED NUMBER,
    ROWS_LOADED NUMBER,
    ERROR_LIMIT NUMBER,
    ERRORS_SEEN NUMBER,
    RET         NUMBER
)
LANGUAGE SQL
AS
$$
    WITH SRC AS (
        SELECT P_JS AS J
    ), BASE AS (
        SELECT f.INDEX                     AS SEQ
             , f.VALUE:file::STRING        AS FILE_NAME
             , f.VALUE:status::STRING      AS STATUS
             , f.VALUE:rows_parsed::NUMBER AS ROWS_PARSED
             , f.VALUE:rows_loaded::NUMBER AS ROWS_LOADED
             , f.VALUE:error_limit::NUMBER AS ERROR_LIMIT
             , f.VALUE:errors_seen::NUMBER AS ERRORS_SEEN
          FROM SRC, LATERAL FLATTEN(INPUT => SRC.J) f
    ), BASE_AGG AS (
        SELECT COUNT(*)                      AS TOT_CNT
             , COUNT_IF(STATUS = 'LOADED')   AS LOADED_CNT
             , COALESCE(SUM(ROWS_PARSED), 0) AS ROWS_PARSED
             , COALESCE(SUM(ROWS_LOADED), 0) AS ROWS_LOADED
             , COALESCE(SUM(ERROR_LIMIT), 0) AS ERROR_LIMIT
             , COALESCE(SUM(ERRORS_SEEN), 0) AS ERRORS_SEEN
          FROM BASE
    )
    SELECT TOT_CNT, LOADED_CNT, ROWS_PARSED, ROWS_LOADED, ERROR_LIMIT, ERRORS_SEEN
         , CASE WHEN TOT_CNT = LOADED_CNT AND ERRORS_SEEN = 0 THEN 0  -- 정상
                WHEN LOADED_CNT = 0 AND ERRORS_SEEN = 0 THEN 2 --처리할 데이터 없음
                ELSE 1 -- 실패
           END AS RET
      FROM BASE_AGG
$$;
--TOT_CNT = 0 : no data files
--TOT_CNT != LOADED_CNT : PARTIALY COPY FAILS : COPY INTO 구문 오류


--사용
-- MERGE 결과(RESULT_SCAN 을 배열로 모은 VARIANT)를 받아 입력/수정/삭제 건수 합계 1행으로 요약
--   키는 Snowflake MERGE 결과 컬럼명: "number of rows inserted/updated/deleted"
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GET_MERGE_RESULT_T(P_JS VARIANT)
RETURNS TABLE (
    ROWS_INSERTED NUMBER,
    ROWS_UPDATED  NUMBER,
    ROWS_DELETED  NUMBER
)
LANGUAGE SQL
AS
$$
    WITH SRC AS (
        SELECT P_JS AS J

    ), BASE AS (
        SELECT f.INDEX                                   AS SEQ
             , f.VALUE:"number of rows inserted"::NUMBER AS ROWS_INSERTED
             , f.VALUE:"number of rows updated"::NUMBER  AS ROWS_UPDATED
             , f.VALUE:"number of rows deleted"::NUMBER  AS ROWS_DELETED
          FROM SRC, LATERAL FLATTEN(INPUT => SRC.J) f
    ), BASE_AGG AS (
         SELECT COALESCE(SUM(ROWS_INSERTED), 0) AS ROWS_INSERTED
              , COALESCE(SUM(ROWS_UPDATED), 0)  AS ROWS_UPDATED
              , COALESCE(SUM(ROWS_DELETED), 0)  AS ROWS_DELETED
           FROM BASE
    )
    SELECT ROWS_INSERTED, ROWS_UPDATED, ROWS_DELETED FROM BASE_AGG
$$;

-- ===== 3. 실행 ID 생성 =====
-- 형식: 'YYYYMMDDHH24MISSFF3_UUID앞8자리' (예: 20261030152611390_62ACCC..). DW_SP_LOG.EXEC_ID 로 사용
CREATE OR REPLACE FUNCTION DW.DW_META.FN_GEN_EXEC_ID()
RETURNS STRING
LANGUAGE SQL
AS
$$
    TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3') || '_' || UPPER(SUBSTR(REPLACE(UUID_STRING(), '-', ''), 1, 8))
$$;

--=========================================================
-- TRANSIENT
-- ===== 4. 처리 이력 테이블 =====
-- DW_PROC_PERIOD : 기간(PERIOD) 적재 처리 이력 (프로시저명, 대상 테이블, 기준일, 건수)
--DROP TABLE DW.DW_META.DW_PROC_PERIOD;
create or replace TABLE DW.DW_META.DW_PROC_PERIOD (
	PROC_DT TIMESTAMP_NTZ,
	SP_NAME VARCHAR(134217728),
	SO_TBL_NAME VARCHAR(134217728),
	TBL_NAME VARCHAR(134217728),
	SN_ETL_EXT_BASE_DT VARCHAR(134217728),
	ROW_COUNT NUMBER(38,0),
    RN_ROW_COUNT NUMBER(38,0)
);

-- drop table DW.DW_META.DW_PROC_PERIOD1;
-- create or replace TABLE DW.DW_META.DW_PROC_PERIOD1 (
-- 	PROC_DT DATE,
-- 	SP_NAME VARCHAR(134217728),
-- 	SO_TBL_NAME VARCHAR,
-- 	TBL_NAME string,
-- 	SN_ETL_EXT_BASE_DT VARCHAR(134217728),
-- 	ROW_COUNT NUMBER(38,0)
-- );

-- 	SP_NAME     VARCHAR(134217728),
-- 	SO_TBL_NAME VARCHAR(16777216),
-- 	TBL_NAME    VARCHAR(16777216),
    
--sp 실행로그
-- DW_SP_LOG : 프로시저 실행당 1행. 시작 시 RUNNING 으로 INSERT, 끝나면 SP_DW_SP_LOG_U 로 결과를 UPDATE
--   EXEC_ID/BAT_ID : 실행 ID / 배치 ID     PROC_TYPE : FULL 또는 PERIOD     PARAMS : 실행 인자(VARIANT)
--   COPY_*  : COPY INTO 단계의 쿼리 ID, 결과 플래그, 파일 수, 적재 건수
--   RAW_*   : 원본 테이블의 삭제/입력/MERGE(입력·수정·삭제)/프루닝 삭제 건수
--   ERROR_* : 실패 시 코드/상태/메시지     REMARK : 비고
create or replace TABLE DW.DW_META.DW_SP_LOG (
    EXEC_ID VARCHAR(134217728),
    BAT_ID VARCHAR(134217728),
    SP_NAME VARCHAR(134217728),
    PROC_TYPE VARCHAR(134217728), -- FULL/PERIOD
    PARAMS           VARIANT,
    EXEC_STEP VARCHAR(134217728),    
    STATUS           VARCHAR(134217728),   -- RUNNING / SUCCESS / FAILED / SKIPPED
    START_TS         TIMESTAMP_NTZ,
    END_TS           TIMESTAMP_NTZ,
    ELAPSED_SEC   NUMBER(18,3),
    COPY_QUERY_ID VARCHAR(134217728),
    COPY_FLAG  NUMBER(38, 0),
    COPY_FILE_CNT  NUMBER(38, 0),
    COPY_LOADED_CNT  NUMBER(38, 0),
    RAW_DELETE_CNT  NUMBER(38, 0),
    RAW_INSERT_CNT  NUMBER(38, 0),
    MERGE_QUERY_ID VARCHAR(134217728),
    RAW_MERGE_INSERT_CNT  NUMBER(38, 0),
    RAW_MERGE_UPDATE_CNT  NUMBER(38, 0),
    RAW_MERGE_DELETE_CNT  NUMBER(38, 0),
    RAW_PRUNING_DELETE_CNT NUMBER(38, 0),
    ERROR_CODE       VARCHAR(134217728),
    ERROR_STATE      VARCHAR(134217728),
    ERROR_MSG        VARCHAR(134217728),
    WAREHOUSE_NAME   VARCHAR(134217728),
    REMARK   VARCHAR(134217728)
);


-- ===== 5. 로그 프로시저 =====
-- SP_DW_SP_LOG_I : 실행 시작 로그 INSERT (STATUS=RUNNING). 현재 주석 처리되어 있어 계정에 생성되지 않는다.
--생성
CREATE OR REPLACE PROCEDURE DW.DW_META.SP_DW_SP_LOG_I(
    P_EXEC_ID                VARCHAR,
    P_BAT_ID                 VARCHAR,
    P_SP_NAME                VARCHAR,
    P_PROC_TYPE              VARCHAR,
    P_PARAMS                 VARIANT,
    P_EXEC_STEP              VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    INSERT INTO DW.DW_META.DW_SP_LOG(EXEC_ID, BAT_ID, SP_NAME, PROC_TYPE, PARAMS, EXEC_STEP, STATUS, START_TS, WAREHOUSE_NAME)
    SELECT :P_EXEC_ID, :P_BAT_ID, :P_SP_NAME, :P_PROC_TYPE, :P_PARAMS, :P_EXEC_STEP, 'RUNNING', CURRENT_TIMESTAMP(), CURRENT_WAREHOUSE();
EXCEPTION
    WHEN OTHER THEN
        -- 로그 실패가 본 적재를 중단시키지 않도록 오류 문자열만 반환
        RETURN 'LOG_ERROR: ' || SQLERRM;
END;
$$;

-- SP_DW_SP_LOG_U : EXEC_ID 로 찾은 로그 행에 단계/상태/건수/오류를 UPDATE (18개 인자)
--   END_TS 는 호출 시각, ELAPSED_SEC 는 START_TS 부터 경과 초(밀리초 단위 계산)
--   로그 갱신 실패가 본 적재를 중단시키지 않도록 예외 시 'LOG_ERROR: ...' 문자열만 반환한다.
CREATE OR REPLACE PROCEDURE DW.DW_META.SP_DW_SP_LOG_U(
    P_EXEC_ID                VARCHAR,
    P_EXEC_STEP              VARCHAR,
    P_STATUS                 VARCHAR,                -- RUNNING / SUCCESS / FAILED / SKIPPED
    P_COPY_QUERY_ID          VARCHAR,
    P_COPY_FLAG              NUMBER(38,0),
    P_COPY_FILE_CNT          NUMBER(38,0),
    P_COPY_LOADED_CNT        NUMBER(38,0),
    P_RAW_DELETE_CNT         NUMBER(38,0),
    P_RAW_INSERT_CNT         NUMBER(38,0),
    P_MERGE_QUERY_ID         VARCHAR,
    P_RAW_MERGE_INSERT_CNT   NUMBER(38,0),
    P_RAW_MERGE_UPDATE_CNT   NUMBER(38,0),
    P_RAW_MERGE_DELETE_CNT   NUMBER(38,0),
    P_RAW_PRUNING_DELETE_CNT NUMBER(38,0),
    P_ERROR_CODE             VARCHAR,
    P_ERROR_STATE            VARCHAR,
    P_ERROR_MSG              VARCHAR,
    P_REMARK                 VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    UPDATE DW.DW_META.DW_SP_LOG
       SET EXEC_STEP              = :P_EXEC_STEP,
           STATUS                 = :P_STATUS,
           END_TS                 = CURRENT_TIMESTAMP(),
           ELAPSED_SEC            = DATEDIFF('millisecond', START_TS, CURRENT_TIMESTAMP()) / 1000,
           COPY_QUERY_ID          = :P_COPY_QUERY_ID,
           COPY_FLAG              = :P_COPY_FLAG,
           COPY_FILE_CNT          = :P_COPY_FILE_CNT,
           COPY_LOADED_CNT        = :P_COPY_LOADED_CNT,
           RAW_DELETE_CNT         = :P_RAW_DELETE_CNT,
           RAW_INSERT_CNT         = :P_RAW_INSERT_CNT,
           MERGE_QUERY_ID         = :P_MERGE_QUERY_ID,
           RAW_MERGE_INSERT_CNT   = :P_RAW_MERGE_INSERT_CNT,
           RAW_MERGE_UPDATE_CNT   = :P_RAW_MERGE_UPDATE_CNT,
           RAW_MERGE_DELETE_CNT   = :P_RAW_MERGE_DELETE_CNT,
           RAW_PRUNING_DELETE_CNT = :P_RAW_PRUNING_DELETE_CNT,
           ERROR_CODE             = :P_ERROR_CODE,
           ERROR_STATE            = :P_ERROR_STATE,
           ERROR_MSG              = :P_ERROR_MSG,
           REMARK                 = :P_REMARK
     WHERE EXEC_ID = :P_EXEC_ID;
EXCEPTION
    WHEN OTHER THEN
        -- 로그 실패가 본 적재를 중단시키지 않도록 오류 문자열만 반환
        RETURN 'LOG_ERROR: ' || SQLERRM;
END;
$$;

    -- EXEC_ID, BAT_ID, SP_NAME, PROC_TYPE, PARAMS, EXEC_STEP, STATUS, START_TS, END_TS, ELAPSED_SEC, COPY_QUERY_ID, COPY_FLAG, COPY_FILE_CNT, COPY_LOADED_CNT, RAW_DELETE_CNT, RAW_INSERT_CNT, RAW_MERGE_INSERT_CNT, RAW_MERGE_UPDATE_CNT, RAW_MERGE_DELETE_CNT, RAW_PRUNING_DELETE_CNT, ERROR_CODE, ERROR_STATE, ERROR_MSG, WAREHOUSE_NAME, REMARK
    -- EXEC_STEP, STATUS, END_TS, ELAPSED_SEC, COPY_QUERY_ID, COPY_FLAG, COPY_FILE_CNT, COPY_LOADED_CNT, RAW_DELETE_CNT, RAW_INSERT_CNT, RAW_MERGE_INSERT_CNT, RAW_MERGE_UPDATE_CNT, RAW_MERGE_DELETE_CNT, RAW_PRUNING_DELETE_CNT, ERROR_CODE, ERROR_STATE, ERROR_MSG, REMARK
    



-- SELECT TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDDHH24MISSFF3') || '_' || UPPER(SUBSTR(REPLACE(UUID_STRING(), '-', ''), 1, 8)), CURRENT_SESSION()::NUMBER, CURRENT_SESSION();

--     2026-10-30 15:26:11:390_62ACCC