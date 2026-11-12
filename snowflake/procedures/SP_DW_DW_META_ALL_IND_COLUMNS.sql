-- ======================================================================
-- ALL_IND_COLUMNS 메타 파일 적재: 스테이지 파일 -> S0(ALL_IND_COLUMNS_S) -> RAW(ALL_IND_COLUMNS)
-- SP_DW_DW_META_ALL_TAB_COLUMNS.sql 과 같은 구조이다. 이 파일은 생성 스크립트로 만들었다.
--   S0  : DW.DW_META.ALL_IND_COLUMNS_S  (파일 내용을 담는 임시 적재 테이블, 실행마다 비운다)
--   RAW : DW.DW_META.ALL_IND_COLUMNS    (기준일 단위로 교체되는 최종 테이블)
-- 스테이지 경로: @DW.DW_SCH.STG/oracle/meta/all_ind_columns/<기준일>/파일.json.gz
-- 컬럼/타입: 스테이지 파일의 INFER_SCHEMA 결과 기준 (10개). 숫자는 NUMBER(38,0), 문자는 VARCHAR, 일시는 TIMESTAMP_NTZ 로 통일했다.
-- 중복 제거 키(같은 기준일 안에서 1건만 남김): INDEX_OWNER+INDEX_NAME+COLUMN_POSITION
-- 전제: UTILS.sql 의 함수/프로시저/로그 테이블이 만들어져 있어야 한다.
--       (FN_GEN_EXEC_ID, FN_GET_SP_NAME, FN_GET_TABLE_NAME, FN_GET_S0_TABLE_NAME,
--        FN_GET_COPY_RESULT_T, SP_DW_SP_LOG_I/U, DW_SP_LOG, DW_PROC_PERIOD)
-- 호출: CALL DW.DW_META.SP_DW_DW_META_ALL_IND_COLUMNS('배치ID');
-- 이 파일은 작성만 했고 Snowflake 에서 실행해 검증하지 않았다.
-- ======================================================================

-- ===== 1. 테이블 =====
-- 주의: createTable.sql 에도 DW.DW_META.ALL_IND_COLUMNS 가 있지만 SN_ETL_* 컬럼이 없고 타입(NUMBER(38,18) 등)이 달라
--       이 프로시저와 맞지 않는다. 기존 테이블이 비어 있다면 아래 DROP 을 직접 실행한 뒤 CREATE 를 실행한다
--       (이 파일은 DROP 을 자동 실행하지 않는다. 데이터가 있으면 DROP 하지 말 것).
-- DROP TABLE DW.DW_META.ALL_IND_COLUMNS;

-- S0 : 파일 원본 + 적재 정보 (임시 데이터라 TRANSIENT)
CREATE TRANSIENT TABLE IF NOT EXISTS DW.DW_META.ALL_IND_COLUMNS_S (
    CHAR_LENGTH                 NUMBER(38, 0),
    COLLATED_COLUMN_ID          VARCHAR(134217728),
    COLUMN_LENGTH               NUMBER(38, 0),
    COLUMN_NAME                 VARCHAR(134217728),
    COLUMN_POSITION             NUMBER(38, 0),
    DESCEND                     VARCHAR(134217728),
    INDEX_NAME                  VARCHAR(134217728),
    INDEX_OWNER                 VARCHAR(134217728),
    TABLE_NAME                  VARCHAR(134217728),
    TABLE_OWNER                 VARCHAR(134217728),
    SN_ETL_FILENAME             VARCHAR(134217728),
    SN_ETL_FILE_ROW_NUMBER      NUMBER(38, 0),
    SN_ETL_FILE_LAST_MODIFIED   TIMESTAMP_NTZ,
    SN_ETL_EXT_BASE_DT          VARCHAR(134217728),
    SN_ETL_PROC_DTTM            TIMESTAMP_LTZ
);

-- RAW : S0 와 같은 컬럼 구성
CREATE TABLE IF NOT EXISTS DW.DW_META.ALL_IND_COLUMNS (
    CHAR_LENGTH                 NUMBER(38, 0),
    COLLATED_COLUMN_ID          VARCHAR(134217728),
    COLUMN_LENGTH               NUMBER(38, 0),
    COLUMN_NAME                 VARCHAR(134217728),
    COLUMN_POSITION             NUMBER(38, 0),
    DESCEND                     VARCHAR(134217728),
    INDEX_NAME                  VARCHAR(134217728),
    INDEX_OWNER                 VARCHAR(134217728),
    TABLE_NAME                  VARCHAR(134217728),
    TABLE_OWNER                 VARCHAR(134217728),
    SN_ETL_FILENAME             VARCHAR(134217728),
    SN_ETL_FILE_ROW_NUMBER      NUMBER(38, 0),
    SN_ETL_FILE_LAST_MODIFIED   TIMESTAMP_NTZ,
    SN_ETL_EXT_BASE_DT          VARCHAR(134217728),
    SN_ETL_PROC_DTTM            TIMESTAMP_LTZ
);

-- ===== 2. 적재 프로시저 =====
CREATE OR REPLACE PROCEDURE DW.DW_META.SP_DW_DW_META_ALL_IND_COLUMNS(
    P_BAT_ID VARCHAR              -- 배치 ID (로그에 기록)
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- 대상 테이블 식별 (프로시저/테이블 이름 생성 함수의 입력)
    V_SP_NAME                VARCHAR;
    V_DATABASE_NAME          VARCHAR DEFAULT 'DW';
    V_SCHEMA_NAME            VARCHAR DEFAULT 'DW_META';
    V_TABLE_NAME             VARCHAR DEFAULT 'ALL_IND_COLUMNS';
    V_EXEC_STEP              VARCHAR DEFAULT 'STEP.0:INIT';

    V_EXEC_ID                VARCHAR;                  -- 실행 ID
    V_PROC_TYPE              VARCHAR DEFAULT 'PERIOD'; -- FULL/PERIOD
    V_PARAMS                 VARIANT;

    -- COPY INTO 결과 변수
    V_COPY_QUERY_ID          VARCHAR;                  -- COPY 문의 쿼리 ID (RESULT_SCAN 대상)
    V_COPY_RET               VARIANT;                  -- COPY 결과 행들을 배열로 모은 JSON
    V_COPY_FLAG              NUMBER(38,0) DEFAULT 0;   -- 0 정상, 1 실패, 2 처리할 파일 없음
    V_COPY_LOADED_CNT        NUMBER(38,0);             -- 적재된 파일 수
    V_COPY_ROWS_PARSED       NUMBER(38,0);             -- 읽은 행 수

    V_STATUS                 VARCHAR;                  -- RUNNING / SUCCESS / FAILED / SKIPPED

    -- RAW 처리 건수 (로그 기록용). MERGE/PRUNING 변수는 이 프로시저에서 쓰지 않아 NULL 로 기록된다
    V_RAW_DELETE_CNT         NUMBER(38,0);
    V_RAW_INSERT_CNT         NUMBER(38,0);
    V_RAW_MERGE_INSERT_CNT   NUMBER(38,0);
    V_RAW_MERGE_UPDATE_CNT   NUMBER(38,0);
    V_RAW_MERGE_DELETE_CNT   NUMBER(38,0);
    V_RAW_PRUNING_DELETE_CNT NUMBER(38,0);
    V_REMARK                 VARCHAR;
    V_LOG_RES                VARCHAR;                  -- 로그 프로시저 반환값
BEGIN
    /*
    ##. 흐름
    DATA FILE 적재 방식 : PERIOD (경로의 날짜 폴더 = 기준일)
    RAW 적재 방식 : REPLACE (같은 기준일을 지우고 다시 넣는다)
    #. S0 : DELETE
    #. S0 : COPY INTO
    #. S0 : 같은 기준일/키 중복 제거 (최신 파일만 남김)
    #. S0 : SUMMARY BASE DATE (처리 이력 기록)
    #. RAW : REPLACE - DELETE / INSERT
    (오래된 데이터 삭제는 구현하지 않았다. 기준일 보관 정책이 정해지면 추가)

    S0  : DW.DW_META.ALL_IND_COLUMNS_S
    RAW : DW.DW_META.ALL_IND_COLUMNS
    */
    --STEP.0:INIT
    V_EXEC_ID := DW.DW_META.FN_GEN_EXEC_ID();
    V_SP_NAME := DW.DW_META.FN_GET_SP_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME);
    V_PARAMS := OBJECT_CONSTRUCT('P_BAT_ID', :P_BAT_ID);

    --LOG INIT
    CALL DW.DW_META.SP_DW_SP_LOG_I(:V_EXEC_ID, :P_BAT_ID, :V_SP_NAME, :V_PROC_TYPE, :V_PARAMS, :V_EXEC_STEP) INTO :V_LOG_RES;

    -- S0 비우기: 이전 실행분을 지우고 이번 파일만 담는다
    V_EXEC_STEP := 'STEP.X:DELETE STAGE TABLE';
    DELETE FROM DW.DW_META.ALL_IND_COLUMNS_S;

    -- 스테이지 폴더(날짜 하위 폴더 포함)의 모든 파일을 S0 로 COPY
    --   SN_ETL_EXT_BASE_DT : 파일 경로 'oracle/meta/<테이블>/<날짜>/파일' 의 4번째 조각 = 날짜 폴더
    --   주의: 이미 적재한 파일은 COPY 이력 때문에 다시 읽히지 않는다 (처리할 파일 없음 -> V_COPY_FLAG = 2)
    --   주의: 윈도우 함수(ROW_NUMBER)는 COPY 변환 안에서 쓰지 않고 적재 후 DELETE 에서 처리한다
    V_EXEC_STEP := 'STEP.X:COPY INTO TO STAGE TABLE';
    COPY INTO DW.DW_META.ALL_IND_COLUMNS_S (
        CHAR_LENGTH, COLLATED_COLUMN_ID, COLUMN_LENGTH, COLUMN_NAME, COLUMN_POSITION, DESCEND, INDEX_NAME, INDEX_OWNER,
        TABLE_NAME, TABLE_OWNER, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT,
        SN_ETL_PROC_DTTM
    )
    FROM (
        SELECT
        $1:CHAR_LENGTH::NUMBER(38, 0), $1:COLLATED_COLUMN_ID::TEXT, $1:COLUMN_LENGTH::NUMBER(38, 0), $1:COLUMN_NAME::TEXT,
        $1:COLUMN_POSITION::NUMBER(38, 0), $1:DESCEND::TEXT, $1:INDEX_NAME::TEXT, $1:INDEX_OWNER::TEXT, $1:TABLE_NAME::TEXT,
        $1:TABLE_OWNER::TEXT
             , METADATA$FILENAME AS SN_ETL_FILENAME
             , METADATA$FILE_ROW_NUMBER AS SN_ETL_FILE_ROW_NUMBER
             , METADATA$FILE_LAST_MODIFIED AS SN_ETL_FILE_LAST_MODIFIED
             , SPLIT_PART(SN_ETL_FILENAME, '/', 4) AS SN_ETL_EXT_BASE_DT
             , CURRENT_TIMESTAMP() AS SN_ETL_PROC_DTTM
          FROM @DW.DW_SCH.STG/oracle/meta/all_ind_columns/ (file_format => 'DW.DW_SCH.FF_JSON_STRIP')
    );

    --STEP.X:COPY RESULT SCAN
    -- 방금 COPY 의 결과를 읽어 FN_GET_COPY_RESULT_T 로 요약한다 (V_COPY_FLAG: 0 정상 / 1 실패 / 2 처리할 파일 없음)
    V_EXEC_STEP := 'STEP.X:COPY RESULT SCAN';
    V_COPY_QUERY_ID := LAST_QUERY_ID();
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(:V_COPY_QUERY_ID));
    SELECT RET, LOADED_CNT, ROWS_PARSED INTO :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON(:V_COPY_RET)));

    -- COPY 가 정상(0)일 때만 RAW 를 갱신한다. 그 외(실패/파일 없음)는 ELSE 에서 비고만 남기고 SUCCESS 로 끝난다
    IF (V_COPY_FLAG = 0) THEN
        -- 기준일(날짜 폴더)별 건수를 처리 이력 테이블에 기록
        --   ROW_COUNT   : S0 에 들어온 전체 행 수 (중복 포함)
        --   RN_ROW_COUNT: 중복 제거 후 남을 행 수 (키 값에 NULL 이 있어도 ARRAY 비교라 한 건으로 센다)
        V_EXEC_STEP := 'STEP.X:GET BASE DATE FROM STAGE TABLE TO DW.DW_META.DW_PROC_PERIOD';
        INSERT INTO DW.DW_META.DW_PROC_PERIOD
        SELECT CURRENT_TIMESTAMP() AS PROC_DT
             , :V_SP_NAME AS SP_NAME
             , DW.DW_META.FN_GET_S0_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS SO_TBL_NAME
             , DW.DW_META.FN_GET_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS TBL_NAME
             , SN_ETL_EXT_BASE_DT
             , COUNT(*) AS ROW_COUNT
             , COUNT(DISTINCT ARRAY_CONSTRUCT(INDEX_OWNER, INDEX_NAME, COLUMN_POSITION)) AS RN_ROW_COUNT
          FROM DW.DW_META.ALL_IND_COLUMNS_S
         GROUP BY SN_ETL_EXT_BASE_DT;

        -- 중복 제거: 같은 기준일에 같은 키(INDEX_OWNER+INDEX_NAME+COLUMN_POSITION)가 여러 파일에 있으면 최신 파일 1건만 남긴다
        --   (SN_ETL_FILENAME + SN_ETL_FILE_ROW_NUMBER 로 행을 특정한다)
        V_EXEC_STEP := 'STEP.X:Cleansing STAGE TABLE';
        DELETE FROM DW.DW_META.ALL_IND_COLUMNS_S
         WHERE (SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER) IN (
               SELECT SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER
                 FROM DW.DW_META.ALL_IND_COLUMNS_S
               QUALIFY ROW_NUMBER() OVER (PARTITION BY SN_ETL_EXT_BASE_DT, INDEX_OWNER, INDEX_NAME, COLUMN_POSITION
                                          ORDER BY SN_ETL_FILE_LAST_MODIFIED DESC, SN_ETL_FILENAME DESC) > 1
         );

        -- RAW 교체 1단계: S0 에 들어온 기준일과 같은 RAW 행을 삭제
        V_EXEC_STEP := 'STEP.X:DELETE RAW USING STAGE TABLE.SN_ETL_EXT_BASE_DT';
        DELETE FROM DW.DW_META.ALL_IND_COLUMNS
         WHERE SN_ETL_EXT_BASE_DT IN (SELECT SN_ETL_EXT_BASE_DT FROM DW.DW_META.ALL_IND_COLUMNS_S GROUP BY SN_ETL_EXT_BASE_DT);
        V_RAW_DELETE_CNT := SQLROWCOUNT;

        --INSERT RAW
        -- RAW 교체 2단계: S0 전체를 RAW 에 입력 (S0 와 RAW 의 컬럼 순서가 같다)
        V_EXEC_STEP := 'STEP.X:INSERT RAW USING STAGE TABLE';
        INSERT INTO DW.DW_META.ALL_IND_COLUMNS
        SELECT * FROM DW.DW_META.ALL_IND_COLUMNS_S;
        V_RAW_INSERT_CNT := SQLROWCOUNT;

        -- 적재가 끝난 S0 비우기 (중간에 실패하면 S0 에 찌꺼기가 남지만, 다음 실행 처음에 비워진다)
        V_EXEC_STEP := 'STEP.X:DELETE STAGE TABLE(AFTER)';
        DELETE FROM DW.DW_META.ALL_IND_COLUMNS_S;
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
        RETURN :V_STATUS || ' @' || :V_EXEC_STEP || ' : ' || :SQLERRM;
END;
$$;

-- ===== 3. 실행 / 확인 =====
-- 실행 전 스테이지에 파일이 있는지 확인
-- LIST @DW.DW_SCH.STG/oracle/meta/all_ind_columns/;
-- 실행
-- CALL DW.DW_META.SP_DW_DW_META_ALL_IND_COLUMNS('TEST_BAT_ID');
-- 결과 확인
-- SELECT * FROM DW.DW_META.DW_SP_LOG ORDER BY EXEC_ID DESC;               -- STATUS, 건수, 오류 메시지
-- SELECT * FROM DW.DW_META.DW_PROC_PERIOD ORDER BY PROC_DT DESC;          -- 기준일별 건수
-- SELECT SN_ETL_EXT_BASE_DT, COUNT(*) FROM DW.DW_META.ALL_IND_COLUMNS GROUP BY 1 ORDER BY 1;
