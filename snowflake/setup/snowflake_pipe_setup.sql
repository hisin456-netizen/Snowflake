-- ======================================================================
-- Snowflake: 스테이지 파일 -> 테이블 적재용 파이프 / 프로시저 (pipeline_snowflake_load DAG 용)
--
-- 구조 (Snowpipe REST API 방식, 공식 문서의 클라이언트 호출 표준)
--   1) DAG 가 SP_PREPARE_LOAD 로 대상 테이블을 정리한다 (FULL: TRUNCATE, PERIOD: base_date 행 DELETE).
--   2) DAG 가 이번 실행의 파일 목록만 REST insertFiles 로 파이프에 전달한다 (snowflake-ingest).
--   3) DAG 가 COPY_HISTORY 로 파일별 적재 결과를 확인한다.
--
-- 대상: SH.PROMOTIONS (FULL), SH.COSTS (PERIOD)  *COSTS_MM, SH_COSTS_YYYY 는 이번 범위 제외
-- 스테이지: @MY_DB.SH.ORACLE_STAGE   (pipeline_config.yaml 의 target.stage)
-- 경로: oracle/data/{schema}/{table}[/{base_date}]/*.json.gz  (S3 키 소문자 규칙과 동일)
--
-- 주의
--  1) 파이프는 AUTO_INGEST 를 쓰지 않는다 (내부 스테이지는 이벤트 알림 불가, REST 로만 호출).
--  2) insertFiles 의 파일 경로는 파이프 FROM 경로(oracle/data/sh/costs/) 기준 상대 경로다.
--  3) 재실행 중복 방지를 위해 적재 파일 경로를 SRC_FILE 컬럼에 저장한다 (METADATA$FILENAME).
--  4) COSTS 컬럼은 oracle_table.xml 의 SELECT 순서/이름 기준으로 가정했다. 실제 DDL 과 다르면 수정한다.
--  5) REST 호출 사용자(키페어 등록 사용자)의 기본 롤에 필요한 권한은 아래 6번 참고 (확인 필요).
-- ======================================================================

USE DATABASE MY_DB;

-- ----------------------------------------------------------------------
-- 1. 파일명 추적 컬럼 (기존 수동 적재 행은 NULL 로 남는다)
-- ----------------------------------------------------------------------
ALTER TABLE SH.PROMOTIONS ADD COLUMN IF NOT EXISTS SRC_FILE VARCHAR;
ALTER TABLE SH.COSTS      ADD COLUMN IF NOT EXISTS SRC_FILE VARCHAR;

-- ----------------------------------------------------------------------
-- 2. 파이프 (AUTO_INGEST = FALSE, 기본값)
--    JSON 키는 소문자 (DAG 가 컬럼명을 소문자로 쓴다). 변환 COPY 로 컬럼을 명시 매핑한다.
-- ----------------------------------------------------------------------
CREATE OR REPLACE PIPE SH.PIPE_PROMOTIONS
AS
COPY INTO SH.PROMOTIONS (
    PROMO_ID, PROMO_NAME, PROMO_SUBCATEGORY, PROMO_SUBCATEGORY_ID,
    PROMO_CATEGORY, PROMO_CATEGORY_ID, PROMO_COST,
    PROMO_BEGIN_DATE, PROMO_END_DATE, PROMO_TOTAL, PROMO_TOTAL_ID, SRC_FILE
)
FROM (
    SELECT
        $1:promo_id::NUMBER(6,0),
        $1:promo_name::VARCHAR,
        $1:promo_subcategory::VARCHAR,
        $1:promo_subcategory_id::NUMBER(38,0),
        $1:promo_category::VARCHAR,
        $1:promo_category_id::NUMBER(38,0),
        $1:promo_cost::NUMBER(10,2),
        $1:promo_begin_date::TIMESTAMP_NTZ,
        $1:promo_end_date::TIMESTAMP_NTZ,
        $1:promo_total::VARCHAR,
        $1:promo_total_id::NUMBER(38,0),
        METADATA$FILENAME
    FROM @MY_DB.SH.ORACLE_STAGE/oracle/data/sh/promotions/
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP);

CREATE OR REPLACE PIPE SH.PIPE_COSTS
AS
COPY INTO SH.COSTS (
    PROD_ID, TIME_ID, PROMO_ID, CHANNEL_ID, UNIT_COST, UNIT_PRICE, SRC_FILE
)
FROM (
    SELECT
        $1:prod_id::NUMBER,
        $1:time_id::TIMESTAMP_NTZ,
        $1:promo_id::NUMBER,
        $1:channel_id::NUMBER,
        $1:unit_cost::NUMBER(10,2),
        $1:unit_price::NUMBER(10,2),
        METADATA$FILENAME
    FROM @MY_DB.SH.ORACLE_STAGE/oracle/data/sh/costs/
)
FILE_FORMAT = (TYPE = JSON COMPRESSION = GZIP);

-- ----------------------------------------------------------------------
-- 3. 프로시저: 적재 전 정리만 한다 (파이프 호출은 DAG 가 REST 로 수행)
--    LOAD_TYPE = FULL   : 테이블 TRUNCATE
--    LOAD_TYPE = PERIOD : base_date 별로 해당 경로에서 적재된 행 DELETE
--    BASE_DATES         : 쉼표 구분 문자열 (예: '20261102,20261103')
--    식별자는 IDENTIFIER() 로 받고 형식을 정규식으로 검증한다.
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SH.SP_PREPARE_LOAD(
    TARGET_TABLE VARCHAR,
    LOAD_TYPE    VARCHAR,
    BASE_DATES   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    bad_arg EXCEPTION (-20001, 'SP_PREPARE_LOAD: 허용되지 않는 인자 값입니다.');
    cleaned INTEGER DEFAULT 0;
    bd VARCHAR;
    pattern VARCHAR;
BEGIN
    IF (NOT REGEXP_LIKE(TARGET_TABLE, '^[A-Za-z0-9_$]+([.][A-Za-z0-9_$]+){0,2}$')) THEN
        RAISE bad_arg;
    END IF;

    IF (UPPER(LOAD_TYPE) = 'FULL') THEN
        TRUNCATE TABLE IDENTIFIER(:TARGET_TABLE);
        cleaned := 1;

    ELSEIF (UPPER(LOAD_TYPE) = 'PERIOD') THEN
        FOR rec IN (SELECT TRIM(value) AS bd FROM TABLE(SPLIT_TO_TABLE(:BASE_DATES, ','))) DO
            bd := rec.bd;
            IF (NOT REGEXP_LIKE(bd, '^[0-9]{4,8}$')) THEN
                RAISE bad_arg;
            END IF;
            pattern := '%/' || bd || '/%';
            DELETE FROM IDENTIFIER(:TARGET_TABLE) WHERE SRC_FILE LIKE :pattern;
            cleaned := cleaned + 1;
        END FOR;

    ELSE
        RAISE bad_arg;
    END IF;

    RETURN 'OK cleaned=' || cleaned;
END;
$$;

-- ----------------------------------------------------------------------
-- 4. REST 호출용 키페어 (한 번만, 계정 관리자 작업)
--    openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--    openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--    ALTER USER <사용자> SET RSA_PUBLIC_KEY = '<rsa_key.pub 본문, 헤더/푸터 제외>';
--    비밀키(rsa_key.p8)는 Airflow Connection extra 에만 둔다 (코드/문서에 적지 않는다).
--
-- 6. 권한 (확인 필요): REST 호출 사용자의 기본 롤(DEFAULT_ROLE)이 사용된다.
--    파이프 OWNERSHIP 또는 OPERATE/MONITOR, 스테이지 USAGE(READ), 대상 테이블 INSERT.
-- ----------------------------------------------------------------------
