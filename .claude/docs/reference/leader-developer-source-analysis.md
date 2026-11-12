# 리드 개발자 소스 분석 (참고용)

> 대상: `leader_developer_source/` (파일 5개, 72KB)
> 분석 방식: **코드를 읽고 정리**했다. 이 소스를 실행하거나 우리 환경에서 검증한 적은 없다.
> 이 문서는 우리 구현(`airflow/dags/source_to_rustfs.py`, `etl_common/`)과 비교하는 참고 자료이며, 소스 자체는 수정하지 않는다.

## 1. 한눈에 보기

**"설정 파일(YAML + XML)만 바꾸면 여러 종류의 DB를 여러 종류의 저장소로 적재할 수 있게 만든 범용 DAG."**
우리 구현과 목표(Oracle -> json.gz -> S3, data/log/meta/translog)는 같지만, **범용성과 설정 분리**를 훨씬 더 밀고 나갔다.

```
leader_developer_source/
├── pipeline_oracle to_s3.py     # DAG 정의 (파일명에 공백이 있음)
├── pipeline_config.yaml         # 공통 설정: 원천/타겟, 경로·파일명 템플릿, 삭제 정책, 통계, translog
├── oracle_table.xml             # 테이블별 설정: SQL, 적재 유형, 기간, 합계 컬럼
└── common/
    ├── config.py                # YAML / XML 읽기와 검증
    └── etl_utils.py             # 실제 적재 로직 전부 (Airflow에 의존하지 않음)
```

| 파일 | 줄 수 | 역할 |
|---|---|---|
| `pipeline_oracle to_s3.py` | 128 | DAG 구조 (태스크 연결만) |
| `common/etl_utils.py` | 982 | 어댑터, 경로·파일명, 기준일 계산, 직렬화, 통계, 적재, translog |
| `common/config.py` | 206 | YAML/XML 로딩, 바인드 변수 탐지, 검증 |
| `pipeline_config.yaml` | 90 | 공통 설정 |
| `oracle_table.xml` | 222 | 테이블 9+4개 설정 |

## 2. DAG 흐름

```
prepare ─────────┬─> build_data_items ─> load_table ────┐
                 │                                       ├─> write_translog
                 └──────────────────────┐                │
build_meta_items ───────────────────────┴─> load_metadata ┘
(prepare 의 결과는 load_table, load_metadata 에도 전달된다. build_meta_items 는 prepare 와 독립이다)
```

| 태스크 | 하는 일 |
|---|---|
| `prepare` | JobID, generation_ts(DAG 실행당 1회), run_date, 현재 시각을 만들어 모든 태스크가 공유 |
| `build_meta_items` | XML `<metadata>` 의 테이블 id 목록을 만듦 |
| `build_data_items` | XML `<data>` 에서 **(테이블, 기준일) 조합**을 펼침. PERIOD 는 기준일마다 항목 1개 |
| `load_metadata` / `load_table` | `.partial(run=run).expand(item=...)` 로 항목마다 태스크 1개. 동시 실행은 `max_active_tis_per_dag=5` |
| `write_translog` | `trigger_rule="all_done"`. 앞의 모든 결과를 모아 **DAG 실행당 파일 1개**로 기록하고, 하나라도 실패면 DAG를 실패 처리 |

## 3. 설정 구조

### 3.1 `pipeline_config.yaml` (공통)
| 섹션 | 내용 |
|---|---|
| `source` | name=`oracle`, system_type=`ORACLE`, connection_id=`oracle_default`, table_config_file |
| `target` | type=`S3`, connection_id=`rustfs_default`, bucket=`airflow-data` |
| `defaults` | max_parallel=5, chunk_size=10000, timezone=`Asia/Seoul`, generation_timestamp 포맷 `%Y%m%d%H%M%S`, job_id 규칙, part_number(시작 1, 8자리) |
| `storage.paths` | data/log/meta/translog 의 경로 템플릿 (예: `{root}/{source}/data/{schema}/{table}/{base_date}`) |
| `storage.filenames` | full / period / meta / translog 파일명 템플릿 |
| `storage.delete_policy` | data·log·meta = 삭제 후 적재, translog = 유지 |
| `statistics` | 파일 단위 통계(건수, NULL 수, SUM), 테이블 전체 집계는 `false` |
| `translog` | enabled, 상태 문자열(`SUCCESS`/`FAILED`) |

### 3.2 `oracle_table.xml` (테이블별)
`<data>` 와 `<metadata>` 두 섹션이 있고, 항목마다 `<id>`, `<schema>`, `<name>`, `<load-type>`, `<auto-load-range>`, `<statistics><sum-columns>`, `<sql>` 를 가진다.

| id | 유형 | 기간 | 비고 |
|---|---|---|---|
| SH_PROMOTIONS | FULL | - | |
| SH_COSTS | PERIOD | `3D` (3일) | 기준일 `YYYYMMDD` |
| SH_COSTS_MM | PERIOD | `3M` (3개월) | 기준일 `YYYYMM` |
| SH_COSTS_YYYY | PERIOD | `3Y` (3년) | 기준일 `YYYY` |
| 메타 9개 | FULL | - | ALL_TABLES ~ ALL_COL_COMMENTS 8개 + `ALL_TAB_PARTITIONS` |

- 메타 SQL 은 XML 에 `WHERE OWNER = 'SH'` 가 **직접 적혀 있다**. `ALL_IND_COLUMNS` 만 `INDEX_OWNER`.
- `ALL_TAB_PARTITIONS` 는 4개 컬럼만 조회하고 **owner 필터가 없다**.
- COSTS 계열 SQL 은 `TIME_ID + (365*4)` 로 날짜를 **약 4년 뒤로 밀어서** 비교한다. 샘플 데이터(2019~2022)를 "오늘 날짜 근처" 데이터처럼 쓰려는 장치로 보인다 (추정).
- `SH_COSTS_MM` 의 `<name>` 은 COSTS_MM 이지만 SQL 은 `SH.COSTS` 를 읽는다. `<name>` 은 경로와 파일명용 라벨이다.

## 4. 핵심 동작 (`etl_utils.py`)

### 4.1 읽기와 분할: pandas 대신 DB-API 커서
```
cur.execute(sql, binds) → cur.fetchmany(chunk) 반복 → 청크마다 gzip NDJSON 생성 → 업로드
```
- 청크 안에서 **NULL 수와 SUM 을 같이 계산**한다 (`encode_chunk`). SUM 은 `Decimal` 로 누적하고 **문자열로 저장**해 정밀도를 유지한다.
- JSON 변환(`_to_jsonable`): 날짜는 `isoformat`, `Decimal` 은 int/float, **bytes 는 hex**, LOB 는 `.read()`.

### 4.2 안전한 삭제 순서
`export_query` 는 **첫 청크를 읽어 SQL 오류가 없는지 확인한 뒤에** 기존 파일을 삭제한다. SQL 이 틀렸을 때 기존 적재분이 사라지지 않게 하려는 의도다 (주석에 명시).

### 4.3 실패를 예외로 던지지 않는다
`run_table_load` / `run_metadata_load` 는 예외를 잡아서 `status=FAILED` 와 오류 메시지가 든 **결과 dict 를 반환**한다. 그래서 실패한 항목도 태스크 자체는 성공으로 끝나고, 마지막 `write_translog` 가 모아서 하나라도 실패면 DAG 를 실패시킨다. 결과가 아예 없는 항목(태스크 비정상 종료)은 `find_missing_results` 가 FAILED 로 채운다.

### 4.4 기준일 계산 (`resolve_base_dates`)
| 단위 | 자동 적재 (오늘 기준) | 기준일 형식 |
|---|---|---|
| D | 오늘 포함 N일 | `YYYYMMDD` |
| M | 이번 달 포함 N개월 | `YYYYMM` |
| Y | 올해 포함 N년 | `YYYY` |

수동 재처리용으로 DAG 파라미터 `base_date_from` / `base_date_to` (YYYYMMDD) 를 받아 **범위의 모든 일/월/연**을 만든다. 둘 중 하나만 주면 오류, 형식·실제 날짜 유효성·순서도 검사한다.

### 4.5 어댑터 구조
| 구분 | 지원 |
|---|---|
| 원천 (`system_type`) | Oracle, PostgreSQL, MySQL, MSSQL, Snowflake |
| 타겟 (`target.type`) | S3 계열(S3, RustFS, MinIO), Snowflake 내부 Stage (`LIST` / `REMOVE` / `PUT`) |

Airflow 에 의존하지 않도록 DB 연결 팩토리와 타겟 어댑터를 호출자가 주입한다.

### 4.6 translog
- DAG 실행당 **파일 1개**, 구조는 `common`(실행 요약) + `items`(테이블·기준일별 결과 배열).
- 하나라도 실패면 `translog/fail/{run_date}`, 전부 성공이면 `success/{run_date}`.
- 항목별 필드: category, table_id, schema, table, base_date, status, file_count, record_count, deleted_count, source_user, 경로, 시작·종료 시각, 소요 초, error(최대 2000자).

### 4.7 입력 검증
- DAG 로딩 시점(`validate_table_config`): 지원하지 않는 바인드 변수(허용은 `BASE_DATE` 뿐), PERIOD 인데 기간 없음/단위 오류, schema 비어 있음, 메타는 FULL 이며 바인드 변수 금지.
- 바인드 변수는 SQL 에서 **정규식으로 자동 탐지**하되 문자열 리터럴(`'...'`) 안은 제외한다.
- 경로·파일명에 들어가는 값은 `_check_vars` 로 허용 문자와 `..` 를 검사한다.

## 5. 우리 구현과의 비교

| 항목 | 리드 개발자 | 우리 구현 |
|---|---|---|
| 설정 | YAML(공통) + XML(테이블) | XML 1개 (경로·파일명 규칙은 코드에 고정) |
| 원천 / 타겟 | 5종 / S3계열 + Snowflake Stage | Oracle / S3 |
| 조회 | DB-API `fetchmany` | pandas `get_df_by_chunks` |
| SUM 계산 | `Decimal`, 문자열 저장 | `float` |
| 기간 단위 | D / M / Y | 일(D)만 |
| 수동 재처리 | `base_date_from` ~ `base_date_to` 범위 | `run_date` 하나 + 최근 N일 |
| 기준일 기준 | 실행 시각(KST) 의 오늘 | `run_date` (비우면 오늘) |
| PERIOD 병렬 단위 | (테이블, 기준일)마다 태스크 | 테이블마다 태스크, 날짜는 안에서 반복 |
| 삭제 시점 | **첫 청크 조회 성공 후** | 조회 전에 삭제 |
| 실패 처리 | 결과 dict 로 반환, 마지막에 DAG 실패 처리 | 태스크가 예외를 던지고 재시도 1회 |
| translog | DAG 실행당 파일 1개 (요약 + 항목 배열) | 테이블마다 파일 1개 |
| S3 키 대소문자 | **소문자로 통일** (`LOWERCASE_S3_KEY`) | 설정 그대로 (대문자) |
| 파일명의 JobID | **없음** (`{schema}_{table}_{generation_ts}_...`) | 있음 (설계 문서 기준) |
| generation_ts 형식 | `%Y%m%d%H%M%S` (`T` 없음) | `%Y%m%dT%H%M%S` |
| 컬럼명 | 조회 결과 그대로 | 소문자로 변환 |
| 메타 owner 필터 | SQL 에 `'SH'` 고정 | `<owner>` 설정으로 코드가 추가 |
| 메타 뷰 수 | 9개 (`ALL_TAB_PARTITIONS` 추가) | 8개 |
| Content-Encoding | `gzip` 헤더 설정 | 설정 안 함 |
| 검증 | 로딩 시 설정 검증 + 경로 값 검사 | 로딩 시 기본 검증 |
| 재시도 / 타임아웃 | 설정 없음 | retries 1, execution_timeout 1시간 |

## 6. 눈에 띄는 점

### 6.1 설계 문서와 다른 부분 (확인 필요)
> 이후 `CreateDag.md` 를 리드 소스에 맞춰 수정했다. 아래는 **수정 전** 문서(`CreateDag.before-leader.md`)와 비교한 내용이다.

- **파일명에 JobID 가 없다.** `CreateDag.md` 4.3 은 `{스키마}_{테이블}_{JobID}_{Generation timestamp}_...` 로 JobID 를 넣도록 되어 있다. 이 소스는 JobID 를 log/translog **내용**에만 넣는다.
- **generation_ts 에 `T` 가 없다.** 설계 문서는 `YYYYMMDDTHHMMSS`, 이 소스는 `YYYYMMDDHHMMSS`.
- **meta 파일명에 스키마가 없다.** `{metadata_table}_{generation_ts}_{part_no}`.
- 이런 차이가 의도인지 단순 불일치인지는 이 소스만으로 알 수 없다.

### 6.2 사용되지 않는 것으로 보이는 설정
읽은 범위에서 코드가 참조하는 곳을 찾지 못한 키: `target.endpoint_url`, `target.region`, `target.format`, `defaults.file.*`, `statistics.scope`, `statistics.table_level_aggregation`, `translog.delete_existing`, `translog.snowpipe_trigger`. 그중 `endpoint_url` 은 YAML 에 적혀 있어도 읽지 않으므로 **Connection 의 extra 에 있어야 동작할 것으로 보인다** (실행해 보지 않았다).

### 6.3 실행 환경에서 부딪힐 수 있는 점 (검증 안 함)
- 설정 파일 위치가 `DAGS_DIR / "pipeline_config.yaml"` 이라 **DAG 파일과 같은 `dags/` 루트**에 있어야 한다. `common/` 도 `dags/` 바로 아래.
- `S3Hook` 은 RustFS 가 요구하는 **path-style 주소 지정**을 기본으로 설정하지 않는다. Connection extra 에 `config_kwargs` 등이 필요할 수 있다 (CLAUDE.md 의 RustFS 규칙 참고).
- Connection ID 가 `oracle_default`, `rustfs_default`, 버킷이 `airflow-data` 로, 우리 환경(`rustfs`, `oracle-raw`)과 다르다.
- DAG 파일명에 공백이 있다 (`pipeline_oracle to_s3.py`).
- 실패해도 태스크가 "성공"으로 보이므로, Airflow 화면의 개별 태스크 색만으로는 실패를 알 수 없고 `write_translog` 의 결과를 봐야 한다.
- `ALL_TAB_PARTITIONS` 의 `HIGH_VALUE_JSON` 컬럼이 우리 Oracle 버전에 있는지는 확인하지 않았다.

## 7. 우리 구현에 참고할 만한 것

| 아이디어 | 이유 |
|---|---|
| 첫 청크를 읽은 뒤에 삭제 | SQL 오류로 기존 적재분이 사라지는 사고를 막는다 |
| `Decimal` 합계를 문자열로 저장 | float 오차 없이 원본 합계와 정확히 비교할 수 있다 |
| `fetchmany` 사용 | pandas 의존이 없고 NULL 수·SUM 을 한 번의 순회로 계산한다 |
| 월(M)·연(Y) 기간 단위 | 월·연 단위 거래 테이블에 필요하다 |
| `base_date_from` / `to` 범위 재처리 | 과거 구간을 한 번에 다시 적재할 수 있다 |
| 경로·파일명 템플릿을 설정으로 분리 | 규칙이 바뀌어도 코드를 고치지 않는다 |
| 경로 값 검사 (`_check_vars`) | 설정 오타나 `..` 로 엉뚱한 경로를 지우는 일을 막는다 |
| `Content-Encoding: gzip` | RustFS 콘솔에서 gzip 파일 미리보기가 된다 |
| 실행당 translog 1파일 | 후속 작업(Snowpipe)이 실행 단위로 결과를 읽기 쉽다 |
| 메타 `ALL_TAB_PARTITIONS` | 파티션 정보까지 필요한지 설계 문서와 대조해 볼 만하다 |

반대로 우리 구현이 나은 점은 재시도·타임아웃 설정, `etl_common` 모듈 분리의 단순함, 컬럼명 소문자 통일, 설정 로딩 시 상세 로그(`cfg`/`ctx` 출력)이다.

## 8. 미확정 / 결정이 필요한 것
1. 파일명에 JobID 를 넣을지 (설계 문서 vs 리드 소스).
2. generation_ts 에 `T` 를 둘지.
3. S3 키를 소문자로 통일할지.
4. translog 를 테이블별 파일로 할지 실행당 1파일로 할지.
5. 실패를 태스크 실패로 드러낼지, 결과 dict + 마지막 DAG 실패로 모을지.
6. 우리 구현을 이 구조에 맞춰 바꿀지, 현재 구조를 유지하며 일부만 가져올지.
