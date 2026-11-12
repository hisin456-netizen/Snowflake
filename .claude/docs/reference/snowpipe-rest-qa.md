# Snowflake 적재 파이프라인 학습 정리 (Q&A)

`pipeline_snowflake_load` DAG 와 `snowflake/setup/snowflake_pipe_setup.sql` 을 읽으면서 나온 질문을 주제별로 정리한 참고 문서다.
설계 기준 문서가 아니다. 구현과 다르게 보이면 코드(`pipeline_snowflake_load.py`, `snowflake_pipe_setup.sql`)가 기준이다.

> 표시 규칙: **(미검증)** 은 코드/문서를 읽고 설명했지만 실제로 실행해 확인하지 않은 내용이다.

---

## 1. 전체 흐름

```
Oracle -> (Airflow 추출) -> RustFS/스테이지의 json.gz 파일
                                  |
DAG: build_items -> prepare_target -> ingest_files -> report
                     |                 |
              SP_PREPARE_LOAD     REST insertFiles + COPY_HISTORY 확인
              (기존 행 정리)         (파이프가 스테이지 파일을 읽어 테이블에 적재)
```

| 단계 | 담당 | 하는 일 |
|---|---|---|
| 정리 | `SH.SP_PREPARE_LOAD` 프로시저 | FULL: `TRUNCATE`, PERIOD: 기준일 행 `DELETE` |
| 적재 지시 | SDK `ingest_files` (REST `insertFiles`) | 이번 실행 파일 목록만 파이프에 전달 |
| 적재 | Snowflake 파이프(`PIPE_PROMOTIONS`, `PIPE_COSTS`) | 스테이지 파일을 읽어 테이블에 INSERT |
| 결과 확인 | DAG `_wait_loaded` | `COPY_HISTORY` 폴링 |

대상: `SH.PROMOTIONS`(FULL), `SH.COSTS`(PERIOD). `COSTS_MM`, `COSTS_YYYY` 는 범위 제외.

---

## 2. 설정 확인 방법 (SQL)

```sql
USE DATABASE MY_DB;
SHOW PIPES IN SCHEMA SH;
SHOW PROCEDURES LIKE 'SP_PREPARE_LOAD' IN SCHEMA SH;
SHOW COLUMNS LIKE 'SRC_FILE' IN TABLE SH.COSTS;
DESC PIPE SH.PIPE_COSTS;
SELECT SYSTEM$PIPE_STATUS('MY_DB.SH.PIPE_COSTS');       -- executionState = RUNNING
LIST @MY_DB.SH.ORACLE_STAGE/oracle/data/sh/costs/;      -- 스테이지에 파일이 있는지

-- 적재 결과
SELECT file_name, status, row_count, first_error_message, last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'COSTS', START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

-- 파일 경로별 행 수 (DELETE 단위 확인)
SELECT SRC_FILE, COUNT(*) FROM MY_DB.SH.COSTS GROUP BY SRC_FILE ORDER BY 1;
```

- REST 방식이라 `pendingFileCount` 가 0 이어도 정상이다.
- `SRC_FILE` 이 전부 `NULL` 이면: 컬럼 추가 후 파이프로 적재된 행이 아직 없다는 뜻이다. 추가 시점은 `QUERY_HISTORY`(`ADD COLUMN ... SRC_FILE`) 또는 `INFORMATION_SCHEMA.TABLES.LAST_DDL` 로 찾는다. `IF NOT EXISTS` 때문에 컬럼이 이미 있었는지는 겉으로 구분되지 않는다. **(미검증)**

---

## 3. SP_PREPARE_LOAD 프로시저

```sql
CREATE OR REPLACE PROCEDURE SH.SP_PREPARE_LOAD(TARGET_TABLE VARCHAR, LOAD_TYPE VARCHAR, BASE_DATES VARCHAR)
RETURNS VARCHAR LANGUAGE SQL EXECUTE AS CALLER
```

| 요소 | 의미 |
|---|---|
| `EXECUTE AS CALLER` | 프로시저 소유자가 아니라 **호출한 사용자의 롤 권한**으로 실행. 호출 롤에 대상 테이블 `DELETE`/`TRUNCATE` 권한 필요. 소유자 권한을 빌려주지 않으므로 권한 우회 통로가 되지 않는다. |
| `IDENTIFIER(:TARGET_TABLE)` | 변수에 든 문자열(`'SH.COSTS'`)을 테이블 이름으로 해석. 이름 자리에는 값을 직접 바인드할 수 없어서 쓴다. |
| `:변수` | SQL 문장 안에서 변수 값을 쓸 때 앞에 붙임. 대입문(`cleaned := 1`)이나 `IF` 조건 안에서는 콜론 없이 씀. |
| `:=` | 변수 대입 |
| `FOR rec IN (쿼리) DO` | 쿼리 결과를 한 행씩 `rec` 에 담아 반복. `rec.컬럼명` 으로 값을 꺼냄. 변수 선언 불필요. |
| `SPLIT_TO_TABLE(:BASE_DATES, ',')` | 쉼표 구분 문자열을 행으로 쪼개는 **테이블 함수**. 컬럼명은 `value`. `FROM TABLE(...)` 로 감싸서 쓴다. FROM 뒤에는 실제 테이블뿐 아니라 행/열을 돌려주는 것이면 올 수 있다. |
| `||` | 문자열 연결 |

### 검증 정규식

| 정규식 | 검사 대상 | 의미 |
|---|---|---|
| `^[A-Za-z0-9_$]+([.][A-Za-z0-9_$]+){0,2}$` | `TARGET_TABLE` | 이름 / `스키마.이름` / `DB.스키마.이름` 형식만 허용. `;`, 공백, 따옴표 거부. `[.]` 는 문자 그대로의 점. |
| `^[0-9]{4,8}$` | `bd`(기준일) | 숫자 4~8자리. `YYYY`/`YYYYMM`/`YYYYMMDD` 를 모두 허용. 날짜 유효성(99999999 등)은 검사하지 않는다. |

### PERIOD 삭제 로직

```sql
pattern := '%/' || bd || '/%';                                   -- '%/20261102/%'
DELETE FROM IDENTIFIER(:TARGET_TABLE) WHERE SRC_FILE LIKE :pattern;
cleaned := cleaned + 1;
```

- 지우는 것은 **Snowflake `SH.COSTS` 테이블의 행**이다. 조건은 데이터의 날짜(`TIME_ID`)가 아니라 `SRC_FILE`(적재 당시 파일 경로)이다.
- Oracle 원천, 스테이지 파일, RustFS 파일은 지워지지 않는다.
- `SRC_FILE` 컬럼 자체는 그대로고 **행 전체**가 삭제된다.
- 같은 기준일을 재처리할 때 중복 적재를 막으려고 먼저 지운다 (delete-insert).
- `SRC_FILE` 이 `NULL` 인 수동 적재 행은 `LIKE` 에 걸리지 않아 지워지지 않는다.
- `cleaned` 는 삭제된 행 수가 아니라 처리한 기준일 개수다. FULL 이면 항상 1.

### 알려진 한계

- 기준일 검증이 루프 안에 있어서, 중간에 잘못된 값이 나오면 앞서 처리한 기준일의 삭제는 이미 반영된 상태다 (`DELETE` 자동 커밋). **(미검증)**
- 테이블 이름은 형식만 검사하고 허용 목록 검사가 없다. 형식이 맞는 다른 테이블 이름을 넘기면 그 테이블도 비워진다. 호출 롤의 권한이 유일한 제한이다.
- 삭제 후 적재가 실패하면 그 기준일 데이터가 빈 상태가 될 수 있다. 같은 기준일로 재실행하면 복구된다.
- `TRUNCATE TABLE` 은 테이블의 `DELETE` 권한이면 실행된다 (`OWNERSHIP` 불필요). `SHOW GRANTS ON TABLE SH.PROMOTIONS;` 로 확인. **(미검증, 공식 문서 재확인 안 함)**

---

## 4. SRC_FILE 과 METADATA$FILENAME

- **스테이지 = 파일을 놓아두는 폴더**, **테이블 = 행/열 데이터**. 둘은 별개다. 스테이지는 테이블이 아니다.
- 파이프의 COPY 문이 스테이지 파일을 읽을 때 두 가지를 얻는다.
  - 파일 안의 정보: `$1:prod_id`, `$1:unit_cost` 등 JSON 내용
  - 파일 밖(이름표) 정보: `METADATA$FILENAME` = 읽고 있는 파일의 경로 (Snowflake 가상 컬럼)
- 마지막 컬럼 `SRC_FILE` 에 `METADATA$FILENAME` 을 저장하므로, 각 행에 "어느 파일에서 왔는지"가 남는다.
- 경로에 기준일 폴더(`.../costs/20261102/...`)가 있어서 `LIKE '%/20261102/%'` 로 기준일 단위 삭제가 가능하다.
- 데이터 날짜(`TIME_ID`)와 경로의 기준일은 같을 수도 다를 수도 있다.

---

## 5. Snowpipe REST API 와 SDK

### 용어 (Snowflake 공식)

- `insertFiles` / `insertReport` / `loadHistoryScan` 은 Snowpipe REST API 의 엔드포인트 이름이다 (직접 만든 말이 아님).
- **REST 엔드포인트 = 문(URL)**, **SDK = 그 문을 열어 주는 도구**. 직접 HTTP 로 호출해도 같은 요청이다.
- SDK(`snowflake-ingest`)가 대신 해 주는 일: URL 조립, **JWT 토큰 생성/서명**, 본문 JSON 변환.
- 파이프는 `AUTO_INGEST` 를 지정하지 않아야 한다(기본 FALSE). 이 프로젝트는 내부 스테이지라 이벤트 알림을 못 쓰기 때문에 REST 방식을 쓴다.

### 핵심 포인트

| 포인트 | 내용 |
|---|---|
| SDK 의 역할 | 파일을 읽거나 올리지 않는다. "이 파일들을 적재해 줘"(지시)와 "결과가 어때?"(조회)만 한다. 파일을 읽어 테이블에 넣는 것은 Snowflake 서버의 파이프. |
| 경로 | `StagedFile` 경로는 파이프 `FROM` 경로 기준 **상대 경로** (`20261102/xxx.json.gz`). 전체 경로를 넣으면 못 찾는다. |
| 한도 | 한 요청에 최대 5,000 파일. (DAG 의 `ingest_batch_size` 1~5000 검증이 이 한도 때문으로 보임) |
| 응답 | `SUCCESS` 는 "접수"일 뿐 적재 완료가 아니다. 결과는 별도 조회(`COPY_HISTORY` 폴링). |
| 데이터 파일 | REST 로 오가지 않는다. 스테이지에 먼저 올라가 있어야 한다. 업로드를 하는 코드는 확인하지 못했다. |
| 암호화 | passphrase 는 **로컬 비공개키 파일**을 보호한다. 전송 구간은 HTTPS. 키페어는 데이터 암호화가 아니라 **신원 증명**용. |

### 키페어

| | 공개키 (`rsa_key.pub`) | 비공개키 (`rsa_key.p8`) |
|---|---|---|
| 보관 | Snowflake 사용자에 등록 | 나만 보관, 절대 공유 안 함 |
| 역할 | 서명 **검증** | 서명 **생성** |
| 첫 줄 | `-----BEGIN PUBLIC KEY-----` | `-----BEGIN PRIVATE KEY-----` |

- Snowpipe REST API 는 키페어(JWT) 인증을 요구하므로 비밀번호로는 호출할 수 없다.
- 비공개키는 요청에 실리지 않는다. 서명된 JWT 만 전송된다.
- SDK 는 비공개키를 읽어서 서명에 **쓰기만** 한다. 키를 만들거나 수정하거나 저장하지 않는다.
- `rsa_key.p8` 은 **처음에는 존재하지 않는다.** 내 PC 터미널에서 `openssl` 로 직접 만든다 (코드가 만들어 주지 않음):

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
```
```sql
ALTER USER <사용자> SET RSA_PUBLIC_KEY = '<rsa_key.pub 본문, 헤더/푸터 제외>';   -- 계정 관리자 작업
DESC USER <사용자>;    -- RSA_PUBLIC_KEY_FP 에 값이 생기면 등록됨
```
- 비공개키는 프로젝트 폴더 밖에 보관하고 코드/문서/채팅에 내용을 붙이지 않는다. DAG 에서는 Airflow Connection `extra` 에만 둔다.

### 키 변환 코드 (`test.py` / `_ingest_manager`)

```
rsa_key.p8 파일 -> read() 바이트 -> load_pem_private_key(+passphrase) 키 객체 -> private_bytes(NoEncryption) PEM 문자열
```
- SDK 가 **"암호 없는 PKCS8 PEM 문자열"** 을 요구해서, 잠긴 키를 암호로 열고 잠금 없이 다시 내보내는 과정이다. 키 내용은 바뀌지 않는다.
- `-nocrypt` 로 만든 키라면 passphrase 는 필요 없고(`None`), 변환은 사실상 형식 맞추기다.
- `get_private_key_passphrase()` 는 키를 만들지 않는다. 이미 정한 암호 문자열을 돌려주는 자리일 뿐이다. 실제 암호를 코드에 직접 쓰지 말고 환경변수 등에서 읽는다.
- 변환된 `private_key_text` 는 암호가 풀린 키이므로 출력/로그 금지.

### SimpleIngestManager 인자

| 인자 | `test.py` | DAG(`_ingest_manager`) |
|---|---|---|
| `account` | 코드에 직접 | Connection `extra.account` |
| `host` | `계정.snowflakecomputing.com` | `extra.ingest_host` 없으면 기본값 |
| `user` | 코드에 직접 | Connection `login` |
| `pipe` | 코드에 직접 | yaml `tables.<id>.pipe` (`DB.SCHEMA.PIPE`) |
| `private_key` | 파일을 코드에서 읽음 | `extra.private_key_content` 또는 `private_key_file` |

객체를 만드는 시점에는 요청이 가지 않는다. 값이 틀리면 실제 요청 시 401(인증)/404(파이프 없음) 등이 난다.

---

## 6. 코드 읽기 메모 (`pipeline_snowflake_load.py`)

### Python 문법: `for tid, sf in SF["tables"].items():`
- `.items()` 는 `(키, 값)` 튜플을 하나씩 내놓고, `tid, sf` 가 그 튜플을 풀어서(언패킹) 받는다. 인자가 두 개가 아니라 값 하나를 두 변수로 받는 것이다.
- yaml `snowflake_load.tables` 기준: `tid`=`SH_PROMOTIONS`/`SH_COSTS`, `sf`=`{target_table, pipe}`.

### `prepare_target`
- `{**item, "stage": "PREPARE", "status": "FAILED", ...}`: `item` 을 복사하고 기본 상태를 FAILED 로 둔다. 성공이 확인될 때만 SUCCESS.
- `CALL {SF['procedure']}(%s, %s, %s)`: 프로시저 이름은 f-string(yaml 값 `SH.SP_PREPARE_LOAD`), 값 세 개는 `%s` 바인드. 객체 이름 자리는 바인드가 안 되기 때문.
- `",".join(item["base_dates"])`: 리스트를 `'20261102,20261103'` 문자열로 바꿔 프로시저의 `BASE_DATES` 에 맞춘다.
- 예외를 던지지 않고 `error` 에 담아 반환 → 마지막에 모아서 기록하고 DAG 실패 처리 (CLAUDE.md 의 실패 처리 설계와 동일).
- 이 태스크는 실행 시 **실제로 TRUNCATE/DELETE 를 수행**한다. 실행 전 대상 테이블과 기준일 확인.

### `_list_relative_files`
- `eu.resolve_prefix(...)`: yaml 경로 템플릿으로 스테이지 폴더 경로를 만든다 (`oracle/data/sh/costs`, 소문자).
- `eu.create_target_adapter(CFG["target"])`: `target.type` 에 맞는 어댑터(S3/RustFS 또는 Snowflake 스테이지)를 만든다. 현재 yaml 의 `target.type` 은 확인하지 않았다.
- PERIOD 는 기준일 폴더만, FULL 은 테이블 폴더 전체를 조회해 `.json.gz` 만 골라 `table_prefix/` 를 잘라 **상대 경로**로 반환한다.
- 이번 실행 파일만 명시적으로 전달하는 이유: 스테이지에 이전 실행 파일이 남아 있을 수 있기 때문.

### `_ingest_manager` 지연 import
- `snowflake.ingest` 를 함수 안에서 import 해서, 패키지가 없는 환경에서도 DAG 파일 로딩은 되도록 한다. 맨 위에서 import 하면 패키지가 없을 때 DAG 자체가 목록에서 사라진다.

---

## 7. 열려 있는 확인 사항

1. **패키지 설치**: `docker-compose.yml` 에서 `snowflake` 문자열이 검색되지 않았다. `snowflake-ingest`, `apache-airflow-providers-snowflake` 가 컨테이너에 설치되는지 확인: `docker exec airflow pip show snowflake-ingest apache-airflow-providers-snowflake`. compose 수정은 사용자 확인 후에만.
2. **권한 설명 불일치**: `snowflake_pipe_setup.sql` 주석 6번은 REST 호출 롤에 스테이지 USAGE / 테이블 INSERT 가 필요하다고 적었으나, 공식 문서 검색 요약은 호출 롤에 파이프 `OPERATE` 만 필요하고 스테이지/테이블 권한은 파이프 소유자 롤 기준이라고 한다. reference 페이지의 권한 항목으로 확인 필요.
3. **스테이지 업로드 담당 코드**: `ingest_files` 는 스테이지를 `LIST` 해서 있는 파일만 전달한다. 스테이지로 파일을 올리는 단계가 어디서 일어나는지 확인하지 못했다.
4. **Connection**: `CFG["target"]["connection_id"]` 에 적힌 ID 로 Snowflake Connection(login, extra.account, 비공개키)을 만들어야 한다.
5. **COSTS 컬럼**: SQL 주석 4번대로 `oracle_table.xml` 기준 추정이다. `DESC TABLE SH.COSTS` 로 실제 DDL 과 대조.

---

## 8. 공식 문서 링크

- [Load data with the Snowpipe REST API](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-overview)
- [Call the Snowpipe REST API with the Snowflake Ingest SDK for Java or Python](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-load)
- [Snowpipe REST API reference](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-apis)
- [Set up the Snowpipe REST API](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-rest-gs)
- [Snowpipe 소개](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro)
- [CREATE PIPE](https://docs.snowflake.com/en/sql-reference/sql/create-pipe)
- [SYSTEM$PIPE_STATUS](https://docs.snowflake.com/en/sql-reference/functions/system_pipe_status)
- [Troubleshooting Snowpipe](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-ts)

## 9. 관련 파일

- `airflow/dags/pipeline_snowflake_load.py` (주석 추가: `_ingest_manager`)
- `snowflake/setup/snowflake_pipe_setup.sql`
- `airflow/dags/pipeline_config.yaml` (`snowflake_load` 섹션)
- `dev/python/test.py` (SDK 샘플, 한국어 주석 추가. `<...>` 빈칸을 채우기 전에는 실행되지 않음)
