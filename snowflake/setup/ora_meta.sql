-- 작업 대상 DB/스키마 지정 (이후 ORA_META 는 DW.DW_META.ORA_META 로 해석된다)
use dw.dw_meta;

-- [참고] ORA_META 컬럼 목록 (Oracle ALL_TAB_COLUMNS 와 동일한 구성, 컬럼 순서 확인용)
-- OWNER,TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_TYPE_MOD, DATA_TYPE_OWNER, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, DEFAULT_LENGTH, DATA_DEFAULT, DATA_DEFAULT_VC, NUM_DISTINCT, LOW_VALUE, HIGH_VALUE, DENSITY, NUM_NULLS, NUM_BUCKETS, LAST_ANALYZED, SAMPLE_SIZE, CHARACTER_SET_NAME, CHAR_COL_DECL_LENGTH, GLOBAL_STATS, USER_STATS, AVG_COL_LEN, CHAR_LENGTH, CHAR_USED, V80_FMT_IMAGE, DATA_UPGRADED, HISTOGRAM, DEFAULT_ON_NULL, DEFAULT_ON_NULL_UPD, RESERVABLE_COLUMN, IDENTITY_COLUMN, EVALUATION_EDITION, UNUSABLE_BEFORE, UNUSABLE_BEGINNING, COLLATION, DOMAIN_OWNER, DOMAIN_NAME, DOMAIN_COLUMN_NAME, DOMAIN_ASSOCIATION_ID, JSON_MODIFIER, VECTOR_INFO
-- [참고] Oracle -> Snowflake 타입 변환 기준 문서
-- https://docs.snowflake.com/en/migrations/aim-for-datawarehouses/code-conversion/translation-references/oracle/sample-data
-- [참고] 매핑해야 할 Oracle 타입 종류 확인용 쿼리 (결과는 파일 하단 목록 참고)
-- SELECT DISTINCT DATA_TYPE FROM ORA_META ORDER BY 1;

-- [목적] ORA_META(Oracle 컬럼 메타)를 읽어 Snowflake 용 CREATE TABLE / DROP TABLE 문을 테이블별로 생성한다.
-- [구조] BASE(CTE) : 컬럼 1개 = 1행으로, Snowflake 컬럼 정의 한 줄('\t컬럼명 타입 [NOT NULL]')을 만든다.
--        최종 SELECT : BASE 를 테이블별로 묶어 컬럼 줄들을 이어 붙이고 DDL 문장으로 감싼다.
WITH BASE AS (
    -- SN_DATA_TYPE1 : Oracle 타입 -> Snowflake 타입 매핑 (매핑 없으면 NULL 이 되어 해당 컬럼이 DDL 에서 빠진다)
    SELECT CASE WHEN DATA_TYPE IN ('CHAR', 'VARCHAR2', 'LONG') THEN 'VARCHAR(134217728)' -- 문자형: 최대 길이
                WHEN DATA_TYPE IN ('NUMBER') AND DATA_PRECISION IS NULL AND DATA_SCALE IS NULL THEN 'NUMBER(38, 18)' -- 정밀도/스케일 미지정 NUMBER
                WHEN DATA_TYPE IN ('NUMBER') AND DATA_SCALE < 0 THEN 'NUMBER(38, 0)' -- DATA_SCALE 음수값
                WHEN DATA_TYPE IN ('NUMBER') THEN 'NUMBER(38, '||DATA_SCALE||')' -- 그 외 NUMBER: 스케일 유지
                WHEN DATA_TYPE IN ('DATE') THEN 'TIMESTAMP' -- Oracle DATE 는 시간 포함이라 TIMESTAMP
                -- Oracle TIMESTAMP 계열은 DATA_TYPE 이 'TIMESTAMP(6)', 'TIMESTAMP(6) WITH TIME ZONE' 처럼 정밀도가 붙어 있어 LIKE 로 매칭한다.
                -- 순서 중요: 더 구체적인 조건(LOCAL TIME ZONE -> TIME ZONE)을 먼저 검사하고 마지막에 일반 TIMESTAMP 를 검사한다.
                WHEN DATA_TYPE LIKE 'TIMESTAMP%WITH LOCAL TIME ZONE' THEN 'TIMESTAMP_LTZ' -- 세션 시간대로 변환되는 타입
                WHEN DATA_TYPE LIKE 'TIMESTAMP%WITH TIME ZONE' THEN 'TIMESTAMP_TZ'       -- 시간대 오프셋 포함
                WHEN DATA_TYPE LIKE 'TIMESTAMP%' THEN 'TIMESTAMP_NTZ'                    -- 시간대 없음
                WHEN DATA_TYPE IN ('RAW') THEN 'BINARY' -- RAW(바이너리) -> BINARY
                ELSE NULL
           END AS SN_DATA_TYPE1
         -- SN_DATA_TYPE2 : NULL 비허용 컬럼이면 ' NOT NULL' 을 붙인다
         , SN_DATA_TYPE1 || (CASE WHEN NULLABLE THEN '' ELSE ' NOT NULL' END) AS SN_DATA_TYPE2
         -- SN_DATA_TYPE3 : 컬럼 정의 한 줄 = 탭 + 컬럼명 + 공백 + 타입
         , '\t'|| COLUMN_NAME || ' ' || SN_DATA_TYPE2 AS SN_DATA_TYPE3
         , TABLE_NAME, COLUMN_ID, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
      FROM ORA_META
    --  WHERE DATA_TYPE IN ('RAW')
     ORDER BY TABLE_NAME, COLUMN_ID
)
-- SELECT * FROM BASE ORDER BY TABLE_NAME, COLUMN_ID;
-- 테이블별(GROUP BY TABLE_NAME) 1행 출력
SELECT LISTAGG(SN_DATA_TYPE3, ',\n') WITHIN GROUP (ORDER BY COLUMN_ID) AS SQL1 -- 컬럼 줄들을 COLUMN_ID 순으로 ',' + 줄바꿈으로 연결
, 'CREATE TABLE DW.DW_META.'|| TABLE_NAME || ' (\n'||SQL1||'\n);\n' AS SQL2    -- CREATE TABLE 문 (이 값을 복사해 실행)
, 'DROP TABLE DW.DW_META.'|| TABLE_NAME ||';' AS DROP_SQL                       -- 되돌릴 때 쓰는 DROP TABLE 문
, TABLE_NAME
FROM BASE
GROUP BY TABLE_NAME
ORDER BY TABLE_NAME
;

-- ORA_META 적재 상태 확인용 전체 조회
select * FROM ORA_META;

-- [확인용] 위 CTE(BASE)와 같은 변환을 컬럼 단위로 확인하는 조회 (DDL 로 묶기 전 단계)
--  - 차이점: 컬럼 줄 앞에 탭('\t')이 없고, TIMESTAMP(NTZ/TZ/LTZ) 매핑 세 줄이 없다.
--  - 타입이 NULL(매핑 없음)로 나오는 컬럼이 있는지, 변환 결과가 맞는지 눈으로 검증할 때 쓴다.
SELECT CASE WHEN DATA_TYPE IN ('CHAR', 'VARCHAR2', 'LONG') THEN 'VARCHAR(134217728)' -- 문자형 -> 최대 길이 VARCHAR
            WHEN DATA_TYPE IN ('NUMBER') AND DATA_PRECISION IS NULL AND DATA_SCALE IS NULL THEN 'NUMBER(38, 18)' -- 정밀도/스케일 미지정 NUMBER
            WHEN DATA_TYPE IN ('NUMBER') AND DATA_SCALE < 0 THEN 'NUMBER(38, 0)' -- DATA_SCALE 음수값
            WHEN DATA_TYPE IN ('NUMBER') THEN 'NUMBER(38, '||DATA_SCALE||')' -- 그 외 NUMBER: 스케일 유지
            WHEN DATA_TYPE IN ('DATE') THEN 'TIMESTAMP' -- Oracle DATE 는 시간 포함
            WHEN DATA_TYPE IN ('RAW') THEN 'BINARY'
            ELSE NULL -- 매핑 없음
       END AS SN_DATA_TYPE1
     , SN_DATA_TYPE1 || (CASE WHEN NULLABLE THEN '' ELSE ' NOT NULL' END) AS SN_DATA_TYPE2 -- NOT NULL 여부 반영
     , COLUMN_NAME || ' ' || SN_DATA_TYPE2 AS SN_DATA_TYPE3 -- 컬럼명 + 타입
     , TABLE_NAME, COLUMN_ID, COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE
  FROM ORA_META
--  WHERE DATA_TYPE IN ('RAW')
 ORDER BY TABLE_NAME, COLUMN_ID;




-- [참고] ORA_META 에 존재하는 DATA_TYPE 목록 (SELECT DISTINCT DATA_TYPE 결과)
-- 모두 위 CASE 에서 매핑된다: CHAR/LONG/VARCHAR2 -> VARCHAR, NUMBER -> NUMBER, DATE -> TIMESTAMP, RAW -> BINARY
--      CHAR
--      DATE
--      LONG
--     NUMBER
-- RAW
--      VARCHAR2