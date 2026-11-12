# Snowflake SQL 파일 분석 (옛 `airflow/dags/sql/mainSql`)

> 2026-11-11 폴더 정리로 파일 위치가 바뀌었다. 이 문서가 분석한 시점의 파일들은 현재 `snowflake/` 아래에 있다
> (`ddl/`, `procedures/`, `utils/`, `setup/`, `scratch/`). 본문의 파일명은 그대로 쓰인다.

대상: 옛 `airflow/dags/sql/mainSql/` 의 13개 파일. 모두 **Snowflake** SQL이며 (Oracle 구문은 `temp.sql` 일부와 주석뿐), S3에 적재된 Oracle 메타데이터(json.gz)를 Snowflake `DW` DB로 적재하는 작업용 스크립트/초안 모음이다. 실행 DAG에서 참조하는 파일은 아니고 작업 노트에 가깝다.

## 전체 구조

```
[Oracle] -> json.gz -> @DW.DW_SCH.STG (스테이지)
   oracle/meta/<테이블명>/<YYYYMMDD>/<테이블명>_<ts>_<part>.json.gz
        |  COPY INTO
        v
DW.DW_META.<TABLE>_S   (스테이징, TRANSIENT)  ->  DW.DW_META.<TABLE> (RAW)
        적재 SP: SP_DW_<DB>_<SCHEMA>_<TABLE>, 실행 로그: DW_SP_LOG / DW_PROC_PERIOD
```

| 영역 | 파일 |
|---|---|
| 계정/환경 구축 | `setup.sql`, `init.sql`, `PAT.sql` |
| 공통 유틸(함수/로그 테이블/SP) | `UTILS.sql` |
| 적재 SP 본체 (버전별) | `EG_SQ.sql`(REPLACE, 프로시저 생성), `eg_meta.sql`(REPLACE, 초안), `eg_merge_metal.sql`(MERGE, 최신) |
| DDL 생성/타입 매핑 | `ora_meta.sql`, `meta.sql`, `DW.DW_META_ALL_TAB_COMMENT.sql` |
| 실험/테스트 | `t1.sql`, `t2.sql`, `temp.sql` |

---

## 1. `setup.sql` — Snowflake 기본 환경 (38줄)
- `ALTER ACCOUNT SET TIMEZONE = 'Asia/Seoul'`, `CURRENT_REGION()`, `CURRENT_TIMESTAMP`
- `CREATE DATABASE DW`, 스키마 `DW_SCH`(스테이지/원본), `DW_META`(메타 적재)
- `FILE FORMAT FF_JSON_STRIP` (JSON, `STRIP_OUTER_ARRAY=TRUE`)
- `STAGE STG` (디렉터리 테이블 활성, `SNOWFLAKE_SSE` 암호화, 기본 포맷 FF_JSON_STRIP)
- `WAREHOUSE DW_WH` (X-SMALL, `AUTO_SUSPEND=120`)
- 확인용: `ls/LIST @DW.DW_SCH.STG/...` (`oracle/data/sh/costs_mm/`, `oracle/meta/all_tab_columns/` 등), `SYSTEM$ALLOWLIST()`, 주석 처리된 `REMOVE`

## 2. `init.sql` — 사용자/권한 + 스키마 추론 (72줄)
- 사용자 `DKBMC` 생성, `ACCOUNTADMIN` 부여, 롤 `ROLE_DEV` 생성/기본 롤 지정
- `INFER_SCHEMA` 로 스테이지의 `all_tab_partitions`, `all_tab_columns` 스키마 추론
- `GENERATE_COLUMN_DESCRIPTION` + `ARRAY_AGG(OBJECT_CONSTRUCT(*))` 로 `CREATE TABLE` DDL 문자열 생성 (`DW_META.all_tab_columns`, `DW_SCH.ALL_CONS_COLUMNS`). 후자는 `REGEXP_REPLACE` 로 `NUMBER(m,n)` → `NUMBER(38,n)` 치환
- `all_tab_partitions` 파일에서 `HIGH_VALUE_CLOB`, `HIGH_VALUE_JSON:high_value` 추출 테스트
- **주의**: 사용자 비밀번호가 평문으로 들어 있다 (값은 여기에 옮기지 않음).

## 3. `PAT.sql` — 서비스 계정 + PAT 인증 (71줄)
1. 네트워크 룰 `NET_RULE_LNL`(`0.0.0.0/0`, 전체 허용), 정책 `NET_POLICY_LNL`, `ALTER ACCOUNT SET NETWORK_POLICY`
2. 인증 정책 `AUTH_POLICY_SVC` (PROGRAMMATIC_ACCESS_TOKEN, 만료 365일)
3. 서비스 사용자 `SVC_BI`(TYPE=SERVICE), 롤 `ROLE_BI`, 웨어하우스/DB/스키마 권한, STG 스테이지 READ/WRITE (FUTURE 포함)
4. PAT `TOKEN_PAT_LNL` 발급 (`ROLE_RESTRICTION='ROLE_BI'`)
5. 점검 쿼리: `DESC USER`, `SHOW USER PROGRAMMATIC ACCESS TOKENS`, `LOGIN_HISTORY_BY_USER`
- **주의**: 주석에 **발급된 PAT 토큰 원문이 그대로** 있다. 폐기/재발급 후 파일에서 제거 권장. 네트워크 룰이 `0.0.0.0/0` 이라 사실상 IP 제한 없음.

## 4. `UTILS.sql` — 공통 함수 / 로그 테이블 / 로그 SP (367줄)
| 객체 | 종류 | 설명 | 상태 |
|---|---|---|---|
| `FN_GET_SP_NAME(db, schema, table)` | 함수 | `SP_<DB>_<SCHEMA>_<TABLE>` | 사용 |
| `FN_GET_TABLE_NAME(...)` | 함수 | `DB.SCHEMA.TABLE` (대문자) | 사용 |
| `FN_GET_S0_TABLE_NAME(...)` | 함수 | 위 결과 + `_S` | 사용 |
| `FN_GET_COPY_RESULT_T(variant)` | 테이블 함수 | COPY 결과 JSON을 집계 → `TOT_CNT, LOADED_CNT, ROWS_PARSED, ROWS_LOADED, ERROR_LIMIT, ERRORS_SEEN, RET`. **RET: 0 정상 / 1 실패 / 2 처리할 파일 없음** | 사용 |
| `FN_GET_MERGE_RESULT_T(variant)` | 테이블 함수 | MERGE 결과에서 `ROWS_INSERTED/UPDATED/DELETED` 추출 | 사용 |
| `FN_GEN_EXEC_ID()` | 함수 | `YYYYMMDDHH24MISSFF3_<UUID 8자>` | 사용 |
| `DW_PROC_PERIOD` | 테이블 | 스테이징에 들어온 기준일자별 건수 기록 (`PROC_DT, SP_NAME, SO_TBL_NAME, TBL_NAME, SN_ETL_EXT_BASE_DT, ROW_COUNT, RN_ROW_COUNT`) | 사용 |
| `DW_SP_LOG` | 테이블 | SP 실행 로그 (25컬럼: 단계, 상태, 시작/종료/소요, COPY·DELETE·INSERT·MERGE·PRUNING 건수, 에러, 웨어하우스, REMARK) | 사용 |
| `SP_DW_SP_LOG_U` | 프로시저 | `EXEC_ID` 행을 갱신(상태/종료시각/건수/에러). 로그 실패 시 `LOG_ERROR:` 문자열만 반환 (적재 중단 방지) | 사용 (18개 파라미터) |
| `SP_DW_SP_LOG_I` | 프로시저 | 로그 INSERT (`RUNNING`) | **주석 처리됨** (이 파일에 활성 정의 없음) |
| `SP_GET_COPY_RESULT`, `SP_GET_COPY_RESULT1`, `FN_GET_COPY_RESULT` (STRING/VARIANT 버전) | 프로시저/함수 | RESULT_SCAN 기반 초기 버전 | 미사용, 주석 처리 |

## 5. `EG_SQ.sql` — 적재 SP 생성 (REPLACE 방식, 초기판) (131줄)
- `CREATE OR REPLACE PROCEDURE DW.DW_META.SP_DW_DW_META_ALL_TAB_COMMENTS(P_BAT_ID)` (`EXECUTE AS CALLER`) — 유일하게 **프로시저로 실제 생성하는** 파일
- 흐름: 로그 INIT → 스테이징 `DELETE` → `COPY INTO ..._S` → `RESULT_SCAN` + `FN_GET_COPY_RESULT_T` 로 결과 판정 → (RET=0이면) `DW_PROC_PERIOD` 기록 → 스테이징에 있는 기준일자의 RAW를 `DELETE` → `INSERT` → 로그 `SUCCESS`, 예외 시 `FAILED` 로그 후 상태 반환
- 한계: 스테이징 컬럼이 `SN_ETL_FILE_CONTENT_KEY`, `SN_ETL_START_SCAN_TIME` 기준이라 최신 DDL(`SN_ETL_PROC_DTTM`, `SN_ETL_RN`)과 다름. 중복 파일 제거(RN) 없음, 오래된 데이터 정리(PRUNING) 없음. 로그 I 호출에 `INTO` 가 없음

## 6. `eg_meta.sql` — REPLACE 방식 익명 블록 (개선판 초안) (294줄)
- `EG_SQ.sql` 의 익명 블록(DECLARE…) 버전. 스테이징 컬럼을 `SN_ETL_PROC_DTTM`, `SN_ETL_RN` 으로 교체
- 추가된 것: `ROW_NUMBER() OVER (PARTITION BY 기준일자,OWNER,TABLE_NAME ORDER BY FILE_LAST_MODIFIED DESC)` 로 같은 기준일자의 중복 파일 중 최신만 사용(`RN>1` 삭제), `RN_ROW_COUNT` 기록, `<= '20250101'` 오래된 데이터 PRUNING
- 하단 주석에 COPY 결과 JSON 샘플(8개 파일, 각 17행), 'NODATA' 케이스, 테스트 쿼리 모음
- **버그**: 63행에 `DW.DW_META.FN_GEN_EXEC_ID` 가 세미콜론/할당 없이 단독으로 남아 있어 그대로 실행하면 구문 오류. `SP_DW_SP_LOG_U` 호출이 17개 인자(`MERGE_QUERY_ID` 누락)라 `UTILS.sql` 의 18개 파라미터 시그니처와도 불일치

## 7. `eg_merge_metal.sql` — MERGE 방식 익명 블록 (최신) (377줄)
- `V_PROC_TYPE = 'PERIOD:MERGE-DELETE'`. 흐름:
  1. 로그 INIT → `COPY INTO ..._S` (RN 계산 포함) → COPY 결과 판정
  2. RET=0: `DW_PROC_PERIOD` 기록 → 스테이징에서 `RN != 1` 삭제 → `MERGE INTO RAW` (키 `OWNER, TABLE_NAME, SN_ETL_EXT_BASE_DT`; 일치 시 UPDATE, 불일치 시 INSERT) → `FN_GET_MERGE_RESULT_T` 로 건수 수집
  3. **원천에서 사라진 행 삭제**: 스테이징에 있는 기준일자 중 `(기준일자, PROC_DTTM)` 이 스테이징과 다른 RAW 행 DELETE
  4. 스테이징 `DELETE`, 그 후 `<= '20250101'` PRUNING, `SP_DW_SP_LOG_U`(18인자)로 `SUCCESS`/`FAILED` 기록
- 하단은 **작업 메모/실험 영역**: 위 MERGE 반복, `WHEN MATCHED AND ... THEN DELETE` 를 쓰는 MERGE 테스트(기준일 `20251201`), `FN_GET_MERGE_RESULT_T` 검증용 쿼리, "prod delete" 후보 쿼리(`기준일자 >= MIN(스테이징)` 범위로 한정), Snowflake 문서의 `merge_example_mult_*` 예제가 섞인 미완성 `INSERT`(구문 오류), 주석 처리된 호출/검증 쿼리, 한국어 메모("한방쿼리로 될까?", PK 설명)
- 설계 메모(주석): PK = `OWNER, TABLE_NAME, SN_ETL_EXT_BASE_DT`, 원천에서 최근 15일치를 가져와 변경/추가/삭제

## 8. `DW.DW_META_ALL_TAB_COMMENT.sql` — ALL_TAB_COMMENTS DDL (41줄)
- `DW.DW_META.ALL_TAB_COMMENTS_S` (`CREATE TRANSIENT TABLE`, 스테이징; 컬럼 `OWNER, TABLE_NAME, TABLE_TYPE, COMMENTS, ORIGIN_CON_ID` + ETL 컬럼 `SN_ETL_FILENAME, SN_ETL_FILE_ROW_NUMBER, SN_ETL_FILE_LAST_MODIFIED, SN_ETL_EXT_BASE_DT, SN_ETL_PROC_DTTM, SN_ETL_RN`)
- `DW.DW_META.ALL_TAB_COMMENTS` (RAW; 위에서 `SN_ETL_RN` 제외)
- 하단: 컬럼 목록 주석과 `METADATA$...` 별칭 조각(단독으로는 실행 불가). 맨 위 `select 1;`, `--#.init` 은 실행 도구용 구분 흔적으로 보임
- 이 파일이 가장 최신 RAW/스테이징 DDL 기준

## 9. `meta.sql` — 메타 RAW 테이블 DDL 일괄 (277줄)
- Oracle 딕셔너리 뷰 8개에 대응하는 `DW.DW_META` 테이블 `CREATE TABLE`:
  `ALL_COL_COMMENTS`, `ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_TABLES`(약 90컬럼), `ALL_TAB_COLUMNS`(약 45컬럼), `ALL_TAB_COMMENTS`
- `ora_meta.sql` 의 매핑 규칙으로 생성한 결과로 보임: 문자 `VARCHAR(134217728)`, 숫자 `NUMBER(38,18)`, `DATE` → `TIMESTAMP`, `RAW` → `BINARY`
- 여기의 `ALL_TAB_COMMENTS` 는 ETL 컬럼이 없는 **구버전**이며 8번 파일과 다르다. `ALL_TAB_PARTITIONS` 등은 없다

## 10. `ora_meta.sql` — Oracle → Snowflake 타입 매핑 DDL 생성기 (59줄)
- 입력은 `ORA_META` (Oracle 딕셔너리 컬럼 정보를 담은 테이블/뷰; 이 폴더에는 정의가 없음)
- `CASE` 매핑: `CHAR/VARCHAR2/LONG` → `VARCHAR(134217728)`, `NUMBER`(정밀도·스케일 NULL) → `NUMBER(38,18)`, 음수 스케일 → `NUMBER(38,0)`, 그 외 `NUMBER(38,<scale>)`, `DATE` → `TIMESTAMP`, `RAW` → `BINARY`
- `LISTAGG` 로 테이블별 `CREATE TABLE` / `DROP TABLE` 문 생성 → `meta.sql` 의 원본
- 참고 코드: `'DATEtime with zone'` 등 TIMESTAMP 변형 분기는 오타성 문자열이라 실제로는 매칭되지 않고, `NULLABLE` 이 'Y'/'N' 문자열이면 `CASE WHEN NULLABLE` 도 Snowflake에서 오류가 날 수 있음. 아래 주석 블록에 등장하는 타입 목록(CHAR, DATE, LONG, NUMBER, RAW, VARCHAR2)이 현재 대상 범위

## 11. `t1.sql` — 타입 변환 메모 + 테스트 DDL (82줄)
- `all_tab_columns1` (3컬럼 테스트 테이블)
- `/* ... */` 안에: 시스템 뷰 컬럼 조회 쿼리(Oracle, `OWNER='SYS'`), 변환 규칙 메모(NUMBER→NUMBER(38,18), JSON→VARIANT, CLOB→VARCHAR 최대 16MB, RAW→BINARY), "JSON/CLOB/RAW 를 json.gz 로 쓸 때 별도 변환이 필요한가" 라는 질문, 전체 컬럼을 TEXT 중심으로 정의한 `DW.DW_SCH.all_tab_columns` DDL (`INFER_SCHEMA` 결과 기반)
- 실행되는 부분은 `CREATE TABLE`, `select 1` 뿐

## 12. `t2.sql` — COPY 실험 및 검증 쿼리 모음 (276줄)
- 스테이지 `LIST`, `COPY INTO DW.DW_SCH.ALL_TAB_COLUMNS ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE`
- `all_tab_comments` 파일 조회(`$1:...`, `METADATA$` 컬럼, `SPLIT_PART(파일명,'/',4)` 로 기준일자 추출)
- 스테이징 `TRUNCATE` 후 `COPY INTO` (**주의**: 경로가 `all_tab_comments1234/` 로 되어 있어 존재하지 않는 경로 → 실패/0건 케이스 시험용으로 보임)
- `RESULT_SCAN` 으로 COPY 결과 조회, `SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY`, `DW_SP_LOG` 조회/`TRUNCATE`
- 기준일자 파싱 `DATE(..., 'YYYYMMDD')`, 파라미터(`S_DT`,`E_DT`) 익명 블록 예제, `ALL_TAB_COLUMNS` 컬럼 확인, Snowflake 문서의 `home_sales` COPY 예제

## 13. `temp.sql` — 임시 실험 (202줄)
- **변경 감지**: `SHA2(COALESCE(..)||'|'||COALESCE(..),256)` 해시 비교 + `FULL OUTER JOIN` 으로 `MATCH/ADDED/DROPPED/MODIFIED` 분류 (T1=어제, T2=오늘 샘플)
- **COPY 결과 분석**: `RESULT_SCAN` 결과를 CTE (`CH`, `CH_CNT`, `CH_ARR`)·`LATERAL FLATTEN` 으로 집계 → `FN_GET_COPY_RESULT_T` 의 원형. 성공/실패 쿼리 ID 하드코딩
- 스테이지 메타 컬럼(`FILE_CONTENT_KEY`, `START_SCAN_TIME`) 중복 확인
- **개인정보 마스킹/암호화 실험**: `SHA2(salt||'|'||값)` 해시 매칭, Snowflake `DECRYPT_RAW`(AES-CBC), Oracle `DBMS_CRYPTO.ENCRYPT`(AES-256-CBC, PKCS5) + `UTL_ENCODE.BASE64_ENCODE` 로 `SH.CUSTOMERS.CUST_STREET_ADDRESS` 암호화 후 Snowflake에서 복호화. 주석에 키 버전 관리·IV 전달 필요 메모
- **주의**: 암·복호화 예제에 키/IV 가 하드코딩되어 있다 (샘플 값으로 보이나 실제 키는 넣지 말 것). Oracle 쪽 IV 는 16바이트, 위쪽 첫 `DECRYPT_RAW` 예제는 8바이트 IV 라 서로 호환되지 않는 시도로 보임

---

## 파일 간 관계 요약

- 적재 SP 의 진화: `EG_SQ.sql`(REPLACE, 단순) → `eg_meta.sql`(REPLACE + 중복제거 + PRUNING) → `eg_merge_metal.sql`(MERGE + 원천 삭제 반영). 실제 구현에서는 `eg_merge_metal.sql` 이 기준.
- 공통 의존: 모든 SP는 `UTILS.sql` 의 함수/테이블/`SP_DW_SP_LOG_U` 와 `setup.sql` 의 스테이지·파일포맷, `DW.DW_META.ALL_TAB_COMMENTS(_S)` DDL 에 의존한다.
- 알려진 불일치: `SP_DW_SP_LOG_I` 가 `UTILS.sql` 에 활성 정의 없음 / `meta.sql` 의 `ALL_TAB_COMMENTS` 가 최신 DDL(`DW.DW_META_ALL_TAB_COMMENT.sql`)과 다름 / `eg_meta.sql` 의 구문 오류와 인자 수 불일치.

## 보안 관련 발견 (조치 권장)
| 파일 | 내용 |
|---|---|
| `PAT.sql` | PAT 토큰 원문이 주석에 남아 있음 → 폐기·재발급, 파일에서 삭제 |
| `PAT.sql` | 네트워크 룰 `0.0.0.0/0` (IP 제한 없음) |
| `init.sql` | 사용자 비밀번호 평문, `ACCOUNTADMIN` 부여 |
| `temp.sql` | AES 키/IV 하드코딩 |

프로젝트 보안 규칙(`.claude/rules/security.md`)에 따라 이 문서에는 실제 비밀값을 옮기지 않았다.
