-- ===== 1. 계정 기본 설정 =====
-- 계정 타임존을 KST로 통일
ALTER ACCOUNT SET TIMEZONE = 'Asia/Seoul';
-- 계정이 위치한 리전 확인
SELECT CURRENT_REGION();
-- 타임존 적용 여부 확인
SELECT CURRENT_TIMESTAMP;

-- ===== 2. DB / 스키마 =====
create database DW;
-- DW_SCH : 스테이지, 파일 포맷, 원본(Oracle 추출분) 테이블
Create schema DW_SCH;
-- DW_META : 메타데이터 RAW/스테이징 테이블, 실행 로그, 공통 함수/프로시저
Create schema DW_META;

-- ===== 3. 파일 포맷 =====
-- JSON 파일 포맷. 최상위 배열([ {...}, {...} ])을 풀어 행 단위로 읽는다.
CREATE OR REPLACE FILE FORMAT FF_JSON_STRIP
  TYPE = JSON
  STRIP_OUTER_ARRAY = TRUE;


-- ===== 4. 스테이지 =====
-- 내부 스테이지 STG : Oracle에서 추출한 json.gz 파일이 올라오는 위치
--   DIRECTORY   : 디렉터리 테이블 활성 (파일 목록을 SQL로 조회 가능)
--   ENCRYPTION  : Snowflake 관리형 서버측 암호화
--   FILE_FORMAT : 기본 포맷을 위 FF_JSON_STRIP 으로 지정
CREATE STAGE STG
	DIRECTORY = ( ENABLE = true )
	ENCRYPTION = ( TYPE = 'SNOWFLAKE_SSE' )
    FILE_FORMAT = FF_JSON_STRIP;

-- 스테이지 속성 확인
desc stage STG;

-- ===== 5. 웨어하우스 =====
-- 가장 작은 사이즈(X-SMALL)로 생성
CREATE OR REPLACE WAREHOUSE DW_WH WITH WAREHOUSE_SIZE='X-SMALL';
-- 120초 유휴 시 자동 중지 (비용 절감)
ALTER WAREHOUSE DW_WH SET AUTO_SUSPEND = 120;

-- ===== 6. 스테이지 파일 확인용 (ls = LIST) =====
-- 경로 규칙: oracle/{data|meta}/{스키마 또는 테이블}/{기준일자}/파일
-- 스테이지 루트 전체
ls @DW.DW_SCH.STG/;

-- 데이터 파일: SH.COSTS_MM 테이블 전체
ls @DW.DW_SCH.STG/oracle/data/sh/costs_mm/;

-- 데이터 파일: SH.COSTS_MM 의 2026년 8월분(기준일자 YYYYMM)
LIST @DW.DW_SCH.STG/oracle/data/sh/costs_mm/202608/;

-- 위 경로 파일 삭제 (재적재 전 정리용. 필요할 때만 주석 해제)
-- REMOVE @DW.DW_SCH.STG/oracle/data/sh/costs_mm/202608/;

-- 메타 파일: ALL_TAB_COLUMNS 의 2026-10-29 기준 추출분
LIST @DW.DW_SCH.STG/oracle/meta/all_tab_columns/20261029/;

-- 메타 파일: ALL_TABLES 의 2026-10-29 기준 추출분
LIST @DW.DW_SCH.STG/oracle/meta/all_tables/20261029/;


-- 데이터 파일: SH_COSTS_YYYY 의 2026년분(기준일자 YYYY)
ls @DW.DW_SCH.STG/oracle/data/sh/sh_costs_yyyy/2026/;

-- Snowflake 접속에 필요한 호스트/포트 허용 목록 (방화벽 설정 참고용)
SELECT SYSTEM$ALLOWLIST();
