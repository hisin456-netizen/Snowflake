-- ======================================================================
-- [주석 안내] 이 블록은 Snowflake Scripting 익명 블록이다 (DECLARE ... BEGIN ... END;).
--   - CREATE PROCEDURE 가 없으므로 저장되지 않고, 이 블록 전체를 실행할 때만 한 번 돈다.
--   - 완성되면 SP_DW_DW_META_ALL_TAB_COMMENTS 라는 프로시저로 만들 용도로 보인다 (파일 아래쪽 주석 참고).
--   - 주석 표기: '--' 는 한 줄 주석, '/* ... */' 는 여러 줄 주석.
--   - 변수 접두사: P_ = 인자(파라미터), V_ = 내부 변수.
--   - SQL 문장 안에서 변수 값을 쓸 때는 ':변수명', 대입문(:=)이나 IF 조건에서는 콜론 없이 쓴다.
--   - '(추정)' 표시는 코드 정의를 확인하지 못해 이름/사용처로 짐작한 내용이다. 실행해 확인한 것이 아니다.
-- 하는 일 요약:
--   스테이지(@DW.DW_SCH.STG)의 json.gz 파일을 스테이지 테이블(ALL_TAB_COMMENTS_S)에 COPY 하고,
--   기준일 단위로 본 테이블(ALL_TAB_COMMENTS)을 지웠다가 다시 채운 뒤, 실행 이력을 로그 테이블에 남긴다.
-- ======================================================================
DECLARE
    -- 배치 ID. 같은 배치로 실행된 작업을 묶어서 추적하기 위한 값. 지금은 아래에서 V_EXEC_ID 로 임시 설정한다.
    P_BAT_ID                 VARCHAR; --추후 인자로..

    -- 이 블록이 속한 프로시저 이름. FN_GET_SP_NAME 이 만들어 주며 로그/이력에 기록된다. (예: SP_DW_DW_META_ALL_TAB_COMMENTS)
    V_SP_NAME                VARCHAR;
    -- 처리 대상 테이블을 구분하는 세 값. 기본값(DEFAULT)이 대상 테이블 DW.DW_META.ALL_TAB_COMMENTS 를 가리킨다.
    V_DATABASE_NAME          VARCHAR DEFAULT 'DW';
    V_SCHEMA_NAME            VARCHAR DEFAULT 'DW_META';
    V_TABLE_NAME             VARCHAR DEFAULT 'ALL_TAB_COMMENTS';
    -- 지금 어느 단계를 실행 중인지 적어 두는 변수. 에러가 나면 이 값이 로그에 남아 "어디서 실패했는지" 알 수 있다.
    V_EXEC_STEP              VARCHAR DEFAULT 'STEP.0:INIT';

    V_EXEC_ID                VARCHAR; --실행 ID  (이번 실행 한 건을 식별하는 고유값)
    -- 적재 유형. FULL(전체), PERIOD(기간/기준일 단위), MERGE(병합) 중 하나. 이 블록은 PERIOD 흐름이다.
    V_PROC_TYPE              VARCHAR DEFAULT 'PERIOD'; -- FULL/PERIOD/MERGE
    -- 실행 인자를 JSON 비슷한 형태(VARIANT, 반정형 데이터)로 묶어 로그에 남기기 위한 변수.
    V_PARAMS                 VARIANT;

    -- COPY INTO 실행의 쿼리 ID. 이 ID 로 COPY 결과(파일별 적재 결과)를 다시 조회한다.
    V_COPY_QUERY_ID          VARCHAR;
    -- COPY 결과 전체를 JSON 배열로 모아 담는 변수.
    V_COPY_RET               VARIANT;
    -- COPY 결과 판정값. 0 이면 처리할 파일이 있었다는 뜻으로 쓰인다 (아래 IF 에서 사용, 정확한 의미는 FN_GET_COPY_RESULT_T 정의 확인 필요).
    V_COPY_FLAG              NUMBER(38,0) DEFAULT 0;
    V_COPY_LOADED_CNT        NUMBER(38,0); --FILE COUNT  (적재된 파일 수)
    V_COPY_ROWS_PARSED       NUMBER(38,0); --ROW COUNT   (읽은 행 수)

    V_STATUS                 VARCHAR; -- RUNNING / SUCCESS / FAILED / SKIPPED  (이번 실행의 최종 상태)
    -- 아래 카운터들은 단계별 처리 건수를 기록한다. 로그 갱신 프로시저(SP_DW_SP_LOG_U)로 넘어간다.
    V_RAW_DELETE_CNT         NUMBER(38,0);  -- 본 테이블에서 지운 행 수
    V_RAW_INSERT_CNT         NUMBER(38,0);  -- 본 테이블에 넣은 행 수
    -- 아래 MERGE 3개는 MERGE 방식용이라 이 PERIOD/REPLACE 흐름에서는 값이 채워지지 않고 NULL 로 로그에 간다.
    V_RAW_MERGE_INSERT_CNT   NUMBER(38,0);
    V_RAW_MERGE_UPDATE_CNT   NUMBER(38,0);
    V_RAW_MERGE_DELETE_CNT   NUMBER(38,0);
    V_RAW_PRUNING_DELETE_CNT NUMBER(38,0);  -- 오래된 데이터 정리(PRUNING)로 지운 행 수
    V_REMARK                 VARCHAR;       -- 비고. 예: 'NO COPY FILES'
    V_LOG_RES                VARCHAR;       -- 로그 프로시저 호출 결과를 받는 변수
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
    -- (위 개요 보충) S0 = 스테이지 테이블(_S, 파일을 임시로 받는 곳), RAW = 최종 본 테이블.
    --   데이터 흐름: 스테이지 파일 -> S0(COPY INTO) -> 중복 정리 -> RAW 에서 해당 기준일 삭제 -> RAW 에 INSERT
    --STEP.0:INIT
    /*
        V_EXEC_ID : 프로시저 실행시간
        P_BAT_ID : 임의 id
        V_SP_NAME : SP_DW_DW_META_ALL_TAB_COMMENTS

    */
    -- 실행 ID 를 만든다. FN_GEN_EXEC_ID 는 함수(값을 돌려줌)이며 정의는 확인하지 못했다.
    -- 주의: 줄 끝의 '#' 은 Snowflake SQL 주석 문법이 아니라서 문법 오류가 날 수 있다 (미검증, 확인 필요).
    V_EXEC_ID := DW.DW_META.FN_GEN_EXEC_ID(); #
    -- 배치 ID 를 실행 ID 와 같은 값으로 임시 설정한다. (나중에 프로시저 인자로 받게 될 부분)
    P_BAT_ID := V_EXEC_ID; --임시설정
    -- 대상 테이블 이름 3개로 프로시저 이름을 만든다. 'SP_' + DB + '_' + 스키마 + '_' + 테이블 (모두 대문자, 공백 제거).
    -- 여기서는 'SP_DW_DW_META_ALL_TAB_COMMENTS' 가 된다. 이름 문자열만 만들며 그런 프로시저가 있는지는 검사하지 않는다.
    V_SP_NAME := DW.DW_META.FN_GET_SP_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME);
    -- 실행 인자를 {"P_BAT_ID": "..."} 형태의 VARIANT 로 묶는다. 로그의 PARAMS 컬럼에 저장된다.
    V_PARAMS := OBJECT_CONSTRUCT('P_BAT_ID', :P_BAT_ID);
    --EG. V_PARAMS := OBJECT_CONSTRUCT('S_DT', :V_S_DT, 'E_DT', :V_E_DT);

    --LOG INIT
    -- 로그 시작 기록: SP_DW_SP_LOG_I(INSERT용 로그 프로시저)를 CALL 한다. 상태 RUNNING 으로 한 행이 생기는 것으로 보인다 (추정).
    -- CALL ... INTO :변수 : 프로시저가 돌려주는 값을 변수 V_LOG_RES 에 받는다. (프로시저는 CALL 로만 호출)
    CALL DW.DW_META.SP_DW_SP_LOG_I(:V_EXEC_ID, :P_BAT_ID, :V_SP_NAME, :V_PROC_TYPE, :V_PARAMS, :V_EXEC_STEP) INTO :V_LOG_RES;

    -- 아래 6줄은 스테이지 테이블에 추가하려던 컬럼 후보를 적어 둔 메모(주석 처리됨)이다.
	-- --SN_ETL_FILE_CONTENT_KEY VARCHAR(134217728),
	-- SN_ETL_FILE_LAST_MODIFIED TIMESTAMP_NTZ,
	-- --SN_ETL_START_SCAN_TIME TIMESTAMP_LTZ,
	-- SN_ETL_EXT_BASE_DT VARCHAR(134217728),
 --    SN_ETL_PROC_DTTM TIMESTAMP_LTZ,
 --    SN_ETL_RN NUMBER(38, 0)

    -- 현재 단계 이름을 갱신한다. 이 아래에서 실패하면 이 이름이 에러 로그에 남는다.
    V_EXEC_STEP := 'STEP.X:COPY INTO TO STAGE TABLE';
    -- [COPY INTO] 스테이지 파일을 읽어 스테이지 테이블(S0)에 넣는다.
    -- 괄호 안은 넣을 컬럼 이름 목록이고, 아래 SELECT 의 값 순서와 1:1 로 대응한다.
    -- 앞 5개 = 파일 내용(OWNER~ORIGIN_CON_ID), 뒤 6개 = 파일/처리 관련 부가 정보(SN_ETL_ 접두사).
    COPY INTO DW.DW_META.ALL_TAB_COMMENTS_S (OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT, SN_ETL_PROC_DTTM, SN_ETL_RN)
    FROM (
        -- $1 은 파일의 한 줄(JSON 한 건). $1:키::타입 은 JSON 에서 그 키 값을 꺼내 타입으로 변환한다.
        SELECT $1:OWNER::TEXT, $1:TABLE_NAME::TEXT, $1:TABLE_TYPE::TEXT, $1:COMMENTS::TEXT, $1:ORIGIN_CON_ID::NUMBER(38, 0)
             -- METADATA$ 로 시작하는 값은 Snowflake 가 파일에 대해 알려 주는 가상 컬럼이다 (파일 안 내용이 아님).
             -- 이 행을 읽은 파일의 경로
             , METADATA$FILENAME AS SN_ETL_FILENAME
             -- 이 행이 파일 안에서 몇 번째 줄인지
             , METADATA$FILE_ROW_NUMBER AS SN_ETL_FILE_ROW_NUMBER
             --, METADATA$FILE_CONTENT_KEY AS SN_ETL_FILE_CONTENT_KEY
             -- 파일의 마지막 수정 시각 (같은 기준일에 파일이 여러 개일 때 최신 파일을 고르는 기준)
             , METADATA$FILE_LAST_MODIFIED AS SN_ETL_FILE_LAST_MODIFIED
             --, METADATA$START_SCAN_TIME AS SN_ETL_START_SCAN_TIME
             -- 파일 경로를 '/' 로 쪼개 4번째 조각을 기준일로 쓴다. 경로가 oracle/meta/<테이블>/<기준일>/파일 형태라는 가정이다 (추정).
             -- 같은 SELECT 안에서 앞에서 붙인 별칭(SN_ETL_FILENAME)을 다시 쓰고 있다.
             , SPLIT_PART(SN_ETL_FILENAME, '/', 4) AS SN_ETL_EXT_BASE_DT
             -- 이 행을 처리한 시각
             , current_timestamp() AS SN_ETL_PROC_DTTM
             -- 같은 (기준일, OWNER, TABLE_NAME) 묶음 안에서 파일 수정 시각이 최신인 행을 1번으로 매긴다.
             -- 같은 기준일 데이터가 여러 파일(재실행 등)에 들어 있을 때 최신 파일의 행만 남기려는 장치이다. (RN = 1 이 최신)
             , row_number() over (partition by SN_ETL_EXT_BASE_DT, $1:OWNER::TEXT, $1:TABLE_NAME::TEXT order by SN_ETL_FILE_LAST_MODIFIED desc) AS SN_ETL_RN
          -- 읽을 위치: 스테이지 DW.DW_SCH.STG 의 해당 경로. file_format 은 JSON 읽기 방식을 정의한 미리 만들어 둔 파일 포맷.
          FROM @DW.DW_SCH.STG/oracle/meta/all_tab_comments/ (file_format => 'DW.DW_SCH.FF_JSON_STRIP')
    );
    --STEP.X:COPY RESULT SCAN
    V_EXEC_STEP := 'STEP.X:COPY RESULT SCAN';
    -- 방금 실행한 COPY INTO 의 쿼리 ID 를 저장한다. (COPY 바로 다음 문장이어야 정확하다)
    V_COPY_QUERY_ID := LAST_QUERY_ID();
    --EG. SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));
    -- RESULT_SCAN(쿼리ID): 그 쿼리가 돌려준 결과(COPY 가 파일별로 보고한 행들: file, status, rows_loaded ...)를 다시 테이블처럼 읽는다.
    -- OBJECT_CONSTRUCT(*): 한 행을 {"컬럼":"값"} JSON 객체로, ARRAY_AGG: 모든 행을 JSON 배열로 모아 V_COPY_RET 에 담는다.
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) INTO :V_COPY_RET FROM TABLE(RESULT_SCAN(:V_COPY_QUERY_ID));
    -- COPY 결과 JSON 을 해석하는 테이블 함수(FN_GET_COPY_RESULT_T)로 판정값(RET), 적재 파일 수, 읽은 행 수를 뽑아 변수 3개에 담는다.
    -- PARSE_JSON : 문자열/VARIANT 를 JSON 으로 해석. 함수의 정의는 확인하지 못했다 (RET 의 정확한 의미는 정의 확인 필요).
    SELECT RET, LOADED_CNT, ROWS_PARSED INTO :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED FROM TABLE(DW.DW_META.FN_GET_COPY_RESULT_T(PARSE_JSON(:V_COPY_RET)));

    -- 처리할 파일이 있었을 때만 본 테이블 갱신 단계로 간다 (판정값 0 = 정상으로 보임, 추정).
    IF (V_COPY_FLAG = 0) THEN
        V_EXEC_STEP := 'STEP.X:GET BASE DATE FROM STAGE TABLE TO DW.DW_META.DW_PROC_PERIOD';
        -- 이번에 들어온 기준일 목록과 건수를 기간 이력 테이블 DW_PROC_PERIOD 에 남긴다.
        -- 컬럼 목록을 쓰지 않았으므로 SELECT 컬럼 순서가 DW_PROC_PERIOD 의 컬럼 순서와 같아야 한다.
        -- FN_GET_S0_TABLE_NAME / FN_GET_TABLE_NAME : 스테이지 테이블명 / 본 테이블명을 만드는 함수 (정의 미확인).
        INSERT INTO DW.DW_META.DW_PROC_PERIOD
        SELECT CURRENT_TIMESTAMP() AS PROC_DT, :V_SP_NAME AS SP_NAME, DW.DW_META.FN_GET_S0_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS SO_TBL_NAME, DW.DW_META.FN_GET_TABLE_NAME(:V_DATABASE_NAME, :V_SCHEMA_NAME, :V_TABLE_NAME) AS TBL_NAME, SN_ETL_EXT_BASE_DT
             -- ROW_COUNT : 기준일별 전체 행 수.
             -- RN_ROW_COUNT : CASE 로 RN=1 인 행만 RN 값(=1)을 더하므로 "중복 제거 후 남을 행 수"와 같다.
             , COUNT(*) AS ROW_COUNT, SUM(CASE WHEN SN_ETL_RN = 1 THEN SN_ETL_RN ELSE NULL END) AS RN_ROW_COUNT
          FROM DW.DW_META.ALL_TAB_COMMENTS_S
         GROUP BY SN_ETL_EXT_BASE_DT;

        V_EXEC_STEP := 'STEP.X:Cleansing STAGE TABLE';
        -- 정리(Cleansing): 같은 키의 최신 행(RN = 1)만 남기고 나머지(RN > 1, 오래된 파일의 중복 행)를 스테이지 테이블에서 지운다.
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS_S WHERE SN_ETL_RN > 1;

        V_EXEC_STEP := 'STEP.X:DELETE RAW USING STAGE TABLE.SN_ETL_EXT_BASE_DT';
        -- 본 테이블에서 이번에 들어온 기준일의 기존 행을 지운다 (지웠다가 다시 넣는 REPLACE 방식, 재실행 시 중복 방지).
        -- 서브쿼리의 GROUP BY 는 기준일 중복을 없앤 목록을 만든다.
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT IN (SELECT SN_ETL_EXT_BASE_DT FROM DW.DW_META.ALL_TAB_COMMENTS_S GROUP BY SN_ETL_EXT_BASE_DT);
        -- SQLROWCOUNT : 직전 DML(INSERT/UPDATE/DELETE)이 처리한 행 수를 담는 Snowflake Scripting 내장 변수.
        V_RAW_DELETE_CNT := SQLROWCOUNT;

        --INSERT RAW
        V_EXEC_STEP := 'STEP.X:INSERT RAW USING STAGE TABLE';
        -- 스테이지 테이블의 행을 본 테이블로 옮긴다. 스테이지에만 있는 SN_ETL_RN 컬럼은 제외하고 10개 컬럼만 넣는다.
        INSERT INTO DW.DW_META.ALL_TAB_COMMENTS
        SELECT OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID, SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT, SN_ETL_PROC_DTTM
          FROM DW.DW_META.ALL_TAB_COMMENTS_S;
        V_RAW_INSERT_CNT := SQLROWCOUNT;

        V_EXEC_STEP := 'STEP.X:DELETE STAGE TABLE';
        -- 본 테이블로 옮겼으니 스테이지 테이블을 비운다 (다음 실행에 대비).
        DELETE FROM DW.DW_META.ALL_TAB_COMMENTS_S;
    ELSE
        -- 처리할 파일이 없었던 경우: 본 테이블은 건드리지 않고 비고만 남긴다.
        V_EXEC_STEP := 'STEP.X:NO COPY RESULT';
        V_REMARK := 'NO COPY FILES';
    END IF;

    V_EXEC_STEP := 'DATA PRUNING - OLD DATA';
    --@@@ SN_ETL_EXT_BASE_DT : DATA TYPE => VARCHAR, DATE, TIMESTAMP AND YYYYMMDD, YYYYMM, YYYY
    -- 데이터 정리(PRUNING): 기준일이 '20250101' 이하인 오래된 행을 본 테이블에서 지운다. (파일 유무와 상관없이 매번 실행)
    -- 기준일이 문자열(VARCHAR)이라 문자열 비교이다. 연 단위 '2025' 는 '20250101' 보다 앞선다고 비교되어 삭제 대상이 될 수 있다 (의도 확인 필요, 미검증).
    DELETE FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT <= '20250101';
    --SELECT * FROM DW.DW_META.ALL_TAB_COMMENTS WHERE SN_ETL_EXT_BASE_DT <= '20250101';
    V_RAW_PRUNING_DELETE_CNT := SQLROWCOUNT;

    V_EXEC_STEP := 'STEP.X:FINISH.SUCCESS';
    V_STATUS := 'SUCCESS';
    -- 로그 종료 기록: SP_DW_SP_LOG_U(UPDATE용 로그 프로시저)로 시작 때 만든 로그 행을 최종 상태와 단계별 건수로 갱신한다.
    -- 인자 순서: 실행ID, 단계, 상태, COPY쿼리ID, COPY판정, 적재파일수, 읽은행수, 삭제수, 입력수, MERGE 3개, PRUNING삭제수,
    --           에러코드/에러상태/에러메시지(성공이라 NULL 3개), 비고
    CALL DW.DW_META.SP_DW_SP_LOG_U(:V_EXEC_ID, :V_EXEC_STEP, :V_STATUS, :V_COPY_QUERY_ID
                                 , :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED, :V_RAW_DELETE_CNT, :V_RAW_INSERT_CNT
                                 , :V_RAW_MERGE_INSERT_CNT, :V_RAW_MERGE_UPDATE_CNT, :V_RAW_MERGE_DELETE_CNT, :V_RAW_PRUNING_DELETE_CNT
                                 , NULL, NULL, NULL, :V_REMARK) INTO :V_LOG_RES;
    -- 최종 상태('SUCCESS')를 호출한 쪽에 돌려준다.
    RETURN :V_STATUS;
-- 위 본문 어디서든 에러가 나면 여기로 건너뛴다 (예외 처리 블록).
EXCEPTION
    WHEN OTHER THEN
        -- 에러 정보를 로그에 FAILED 로 남긴다. V_EXEC_STEP 에는 에러가 난 시점의 단계 이름이 들어 있다.
        -- SQLCODE / SQLSTATE / SQLERRM : 직전 에러의 코드 / 상태 / 메시지를 담는 Snowflake Scripting 내장 변수.
        V_STATUS := 'FAILED';
        CALL DW.DW_META.SP_DW_SP_LOG_U(:V_EXEC_ID, :V_EXEC_STEP, :V_STATUS, :V_COPY_QUERY_ID
                                     , :V_COPY_FLAG, :V_COPY_LOADED_CNT, :V_COPY_ROWS_PARSED, :V_RAW_DELETE_CNT, :V_RAW_INSERT_CNT
                                     , :V_RAW_MERGE_INSERT_CNT, :V_RAW_MERGE_UPDATE_CNT, :V_RAW_MERGE_DELETE_CNT, :V_RAW_PRUNING_DELETE_CNT
                                     , :SQLCODE, :SQLSTATE, :SQLERRM, :V_REMARK) INTO :V_LOG_RES;
        -- 에러를 다시 던지지 않고 'FAILED' 문자열을 반환한다. 호출한 쪽은 에러가 아니라 반환값으로 실패를 알게 된다.
        RETURN :V_STATUS;
END;

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