-- ======================================================================
-- 적재 프로시저 예시: 스테이지의 ALL_TAB_COMMENTS 메타 파일 -> S0 테이블 -> RAW 테이블
--   S0  : DW.DW_META.ALL_TAB_COMMENTS_S  (파일 내용을 그대로 담는 임시 적재 테이블)
--   RAW : DW.DW_META.ALL_TAB_COMMENTS    (기준일 단위로 교체되는 최종 테이블)
-- 전제: 위 두 테이블은 미리 만들어져 있어야 한다 (이 파일에는 CREATE TABLE 이 없음).
--       로그 프로시저 SP_DW_SP_LOG_I / SP_DW_SP_LOG_U 와 DW_SP_LOG 테이블도 필요 (UTILS.sql).
-- 호출: CALL DW.DW_META.SP_DW_DW_META_ALL_TAB_COMMENTS('배치ID');
-- ======================================================================
CREATE OR REPLACE PROCEDURE DW.DW_META.SP_DW_DW_META_ALL_TAB_COMMENTS(
    P_BAT_ID VARCHAR              -- 배치 ID (로그에 기록)
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- 대상 테이블 식별 (프로시저/테이블 이름 생성 함수의 입력)
    V_SP_NAME       VARCHAR;
    V_DATABASE_NAME VARCHAR DEFAULT 'DW';
    V_SCHEMA_NAME   VARCHAR DEFAULT 'DW_META';
    V_TABLE_NAME    VARCHAR DEFAULT 'ALL_TAB_COMMENTS';
    V_EXEC_STEP              VARCHAR DEFAULT 'STEP.0:INIT';

    V_EXEC_ID                VARCHAR; --실행 ID
    V_PROC_TYPE              VARCHAR DEFAULT 'PERIOD';                -- FULL/PERIOD
    V_PARAMS                 VARIANT;

    -- COPY INTO 결과 변수
    V_COPY_QUERY_ID          VARCHAR;                 -- COPY 문의 쿼리 ID (RESULT_SCAN 대상)
    -- V_QID     VARCHAR;
    V_COPY_RET VARIANT;                               -- COPY 결과 행들을 배열로 모은 JSON
    V_COPY_FLAG NUMBER(38,0) DEFAULT 0;               -- 0 정상, 1 실패, 2 처리할 데이터 없음
    V_COPY_LOADED_CNT        NUMBER(38,0); --FILE COUNT
    V_COPY_ROWS_PARSED       NUMBER(38,0); --ROW COUNT

    --V_INS_CNT NUMBER DEFAULT 0;

    V_STATUS                 VARCHAR;                -- RUNNING / SUCCESS / FAILED / SKIPPED
    V_START_TS               TIMESTAMP_NTZ;
    V_END_TS                 TIMESTAMP_NTZ;
    

    -- RAW 테이블 처리 건수 (로그 기록용). MERGE/PRUNING 변수는 이 프로시저에서는 쓰지 않아 NULL 로 기록된다
    V_RAW_DELETE_CNT         NUMBER(38,0);
    V_RAW_INSERT_CNT         NUMBER(38,0);
    V_RAW_MERGE_INSERT_CNT   NUMBER(38,0);
    V_RAW_MERGE_UPDATE_CNT   NUMBER(38,0);
    V_RAW_MERGE_DELETE_CNT   NUMBER(38,0);
    V_RAW_PRUNING_DELETE_CNT NUMBER(38,0);
    V_REMARK                 VARCHAR;

     V_LOG_RES VARCHAR;
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
    (참고: 위 흐름의 마지막 PRUNING 단계는 아래 코드에 구현되어 있지 않다)

    S0 : DW.DW_META.ALL_TAB_COMMENTS_S
    RAW : DW.DW_META.ALL_TAB_COMMENTS
    */
    --STEP.0:INIT
    -- 실행 ID, 프로시저 이름, 실행 인자를 만들고 로그 행을 RUNNING 으로 시작한다
    V_EXEC_ID := DW.DW_META.FN_GEN_EXEC_ID();
    --P_BAT_ID := V_EXEC_ID; --임시설정
    V_SP_NAME := DW.DW_META.FN_GET_SP_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME);
    V_PARAMS := OBJECT_CONSTRUCT('P_BAT_ID', :P_BAT_ID);
    --EG. V_PARAMS := OBJECT_CONSTRUCT('S_DT', :V_S_DT, 'E_DT', :V_E_DT);
    
    --LOG INIT
    -- 주의: SP_DW_SP_LOG_I 는 UTILS.sql 에서 주석 처리되어 있다. 계정에 없으면 여기서 실패한다
    CALL DW.DW_META.SP_DW_SP_LOG_I(:V_EXEC_ID, :P_BAT_ID, :V_SP_NAME, :V_PROC_TYPE, :V_PARAMS, :V_EXEC_STEP);

    -- S0 비우기: 이전 실행분을 지우고 이번 파일만 담는다
    V_EXEC_STEP := 'STEP.X:DELETE STAGE TABLE';
    DELETE FROM DW.DW_META.ALL_TAB_COMMENTS_S;

    -- 스테이지 폴더(날짜 하위 폴더 포함)의 모든 파일을 S0 로 COPY
    --   $1:컬럼 : JSON 키를 컬럼 타입으로 변환
    --   METADATA$* : 파일명/행 번호/수정 시각 등 Snowflake 가 주는 파일 메타 (SN_ETL_* 컬럼에 보관)
    --   SN_ETL_EXT_BASE_DT : 파일 경로 'oracle/meta/<테이블>/<날짜>/파일' 의 4번째 조각 = 날짜 폴더
    --   주의: 폴더에 여러 날짜의 파일이 있으면 모두 읽힌다. 이미 읽은 파일은 COPY 이력으로 재적재되지 않을 수 있다
    V_EXEC_STEP := 'STEP.X:COPY INTO TO STAGE TABLE';
    COPY INTO DW.DW_META.ALL_TAB_COMMENTS_S (OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_CONTENT_KEY, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_START_SCAN_TIME, SN_ETL_EXT_BASE_DT)
    FROM (
        SELECT $1:OWNER::TEXT, $1:TABLE_NAME::TEXT, $1:TABLE_TYPE::TEXT, $1:COMMENTS::TEXT, $1:ORIGIN_CON_ID::NUMBER(38, 0)
             , METADATA$FILENAME AS SN_ETL_FILENAME
             , METADATA$FILE_ROW_NUMBER AS SN_ETL_FILE_ROW_NUMBER
             , METADATA$FILE_CONTENT_KEY AS SN_ETL_FILE_CONTENT_KEY
             , METADATA$FILE_LAST_MODIFIED AS SN_ETL_FILE_LAST_MODIFIED
             , METADATA$START_SCAN_TIME AS SN_ETL_START_SCAN_TIME
             , SPLIT_PART(SN_ETL_FILENAME, '/', 4) AS SN_ETL_EXT_BASE_DT
          FROM @DW.DW_SCH.STG/oracle/meta/all_tab_comments/ (file_format => 'DW.DW_SCH.FF_JSON_STRIP')
    );
    --STEP.X:COPY RESULT SCAN
    -- 방금 COPY 의 결과를 읽어 FN_GET_COPY_RESULT_T 로 요약한다 (V_COPY_FLAG: 0 정상 / 1 실패 / 2 처리할 파일 없음)
    V_EXEC_STEP := 'STEP.X:COPY RESULT SCAN';
    V_COPY_QUERY_ID := LAST_QUERY_ID();
    --EG. SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(:V_COPY_QUERY_ID));
    SELECT RET, LOADED_CNT, ROWS_PARSED INTO :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON(:V_COPY_RET)));

    -- COPY 가 정상(0)일 때만 RAW 를 갱신한다. 그 외(실패/파일 없음)는 아래 ELSE 에서 비고만 남기고 SUCCESS 로 끝난다
    IF (V_COPY_FLAG = 0) THEN
        -- 기준일(날짜 폴더)별 건수를 처리 이력 테이블에 기록
        V_EXEC_STEP := 'STEP.X:GET BASE DATE FROM STAGE TABLE TO DW.DW_META.DW_PROC_PERIOD';
        INSERT INTO DW.DW_META.DW_PROC_PERIOD
        SELECT CURRENT_TIMESTAMP() AS PROC_DT, :V_SP_NAME AS SP_NAME, DW.DW_META.FN_GET_S0_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS SO_TBL_NAME, DW.DW_META.FN_GET_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS TBL_NAME, SN_ETL_EXT_BASE_DT, COUNT(*) AS ROW_COUNT
          FROM DW.DW_META.ALL_TAB_COMMENTS_S
         GROUP BY SN_ETL_EXT_BASE_DT;
    
        -- RAW 교체 1단계: S0 에 들어온 기준일과 같은 RAW 행을 삭제
        V_EXEC_STEP := 'STEP.X:DELETE RAW USING STAGE TABLE.SN_ETL_EXT_BASE_DT';
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT IN (SELECT SN_ETL_EXT_BASE_DT FROM DW.DW_META.ALL_TAB_COMMENTS_S GROUP BY SN_ETL_EXT_BASE_DT);
        V_RAW_DELETE_CNT := SQLROWCOUNT;

        --INSERT RAW
        -- RAW 교체 2단계: S0 전체를 RAW 에 입력 (S0 와 RAW 의 컬럼 순서가 같아야 한다)
        V_EXEC_STEP := 'STEP.X:INSERT RAW USING STAGE TABLE';
        INSERT INTO DW.DW_META.ALL_TAB_COMMENTS
        SELECT OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_CONTENT_KEY, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_START_SCAN_TIME, SN_ETL_EXT_BASE_DT
          FROM DW.DW_META.ALL_TAB_COMMENTS_S;
        V_RAW_INSERT_CNT := SQLROWCOUNT;
    ELSE
        V_EXEC_STEP := 'STEP.X:NO COPY RESULT';
        V_REMARK := 'NO COPY FILES';
    END IF;
          
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
        RETURN :V_STATUS;
END;
$$;