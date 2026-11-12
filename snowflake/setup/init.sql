-- 이후 쿼리의 기본 DB.스키마 지정
use DW.DW_SCH;

-- ===== 1. 개발용 사용자 / 롤 =====
-- 사용자 DKBMC 생성 (이미 있으면 건너뜀). 기본 웨어하우스 COMPUTE_WH, 최초 로그인 시 비밀번호 변경 요구 안 함
-- 주의: 비밀번호가 평문으로 들어 있음. 실제 운영에서는 이 파일에 두지 말 것
CREATE USER IF NOT EXISTS DKBMC PASSWORD='Welcome1!DKBMC' DEFAULT_WAREHOUSE = COMPUTE_WH MUST_CHANGE_PASSWORD = FALSE;

-- 사용자에게 ACCOUNTADMIN 롤 부여 (계정 최고 권한)
GRANT ROLE ACCOUNTADMIN TO USER DKBMC;

-- 개발용 롤 ROLE_DEV 생성 후 ACCOUNTADMIN 아래 계층에 연결
CREATE ROLE IF NOT EXISTS ROLE_DEV;
GRANT ROLE ROLE_DEV TO ROLE ACCOUNTADMIN;
-- 로그인 시 기본으로 쓰는 롤을 ROLE_DEV 로 지정 (평소엔 낮은 권한으로 작업)
ALTER USER DKBMC SET DEFAULT_ROLE = ROLE_DEV;

-- ===== 2. 스테이지 파일에서 스키마 추론 =====
-- all_tab_partitions 파일의 컬럼명/타입을 자동 추론해서 조회
SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION    => '@DW.DW_SCH.STG/oracle/meta/all_tab_partitions/',
    FILE_FORMAT => 'DW.DW_SCH.FF_JSON_STRIP'
  )
);

-- all_tab_columns 파일의 추론 결과를 컬럼 정의 문자열(columns_ddl)로 변환
SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'table') AS columns_ddl
FROM TABLE(
  INFER_SCHEMA(
    LOCATION    => '@DW.DW_SCH.STG/oracle/meta/all_tab_columns/',
    FILE_FORMAT => 'DW.DW_SCH.FF_JSON_STRIP'
  )
);

-- ===== 3. CREATE TABLE DDL 자동 생성 =====
-- all_tab_columns 용 CREATE TABLE 문을 만들어 출력 (컬럼은 ORDER_ID 순, 큰따옴표 제거)
SELECT 'CREATE TABLE DW.DW_META.all_tab_columns ('
       || REPLACE(
         GENERATE_COLUMN_DESCRIPTION(
           ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID),
           'table'
         ),
         '"', ''
       ) || ');' AS create_table_ddl
FROM TABLE(
  INFER_SCHEMA(
    LOCATION    => '@DW.DW_SCH.STG/oracle/meta/all_tab_columns/',
    FILE_FORMAT => 'DW.DW_SCH.FF_JSON_STRIP'
  )
);


-- 위와 같은 방식 + NUMBER(m,n) 을 NUMBER(38,n) 으로 통일해서 DDL 생성
-- 주의: 테이블명은 ALL_CONS_COLUMNS 인데 읽는 경로는 all_tab_columns/ 임 (확인 필요)
SELECT 'CREATE TABLE DW.DW_SCH.ALL_CONS_COLUMNS ('
       || REGEXP_REPLACE(
            REPLACE(
              GENERATE_COLUMN_DESCRIPTION(
                ARRAY_AGG(OBJECT_CONSTRUCT(*)) WITHIN GROUP (ORDER BY ORDER_ID),
                'table'
              ),
              '"', ''
            ),
            $$NUMBER\s*\(\s*\d+\s*,\s*(\d+)\s*\)$$,   -- NUMBER(m, n) 에서 n 캡처
            $$NUMBER(38, \1)$$,                         -- m 을 38 로 치환
            1, 0, 'i'                                   -- 전체 치환, 대소문자 무시
          )
       || ');' AS create_table_ddl
FROM TABLE(
  INFER_SCHEMA(
    LOCATION    => '@DW.DW_SCH.STG/oracle/meta/all_tab_columns/',
    FILE_FORMAT => 'DW.DW_SCH.FF_JSON_STRIP'
  )
);


-- ===== 4. 파티션 high_value JSON 컬럼 추출 테스트 =====
-- 특정 파일 한 개에서 HIGH_VALUE_CLOB(원문), HIGH_VALUE_JSON(VARIANT), 그 안의 high_value 값 확인
select $1:HIGH_VALUE_CLOB::TEXT
, $1:HIGH_VALUE_JSON::VARIANT as aaa
--, $1:HIGH_VALUE_JSON[0][high_value]::TEXT
, $1:HIGH_VALUE_JSON:high_value::TEXT AS extracted_high_value
, $1
from @DW.DW_SCH.stg/oracle/meta/all_tab_partitions/20261027/all_tab_partitions_20261027144446_00000001.json.gz
;
