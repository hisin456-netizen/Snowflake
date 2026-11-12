# Oracle -> RustFS 적재 DAG: 구현 현황 (리드 소스 기준)

> 이 구현은 리드 개발자 소스(`leader_developer_source/`)를 기준으로 한다. 상세 분석은 `../reference/leader-developer-source-analysis.md`.
> 설계 기준 문서는 `../CreateDag.md` 이며 리드 소스에 맞춰 수정되어 구현과 일치한다.
> 코드가 바뀌면 이 문서도 같이 고친다.

## 1. 한눈에 보기

**Oracle 의 표 데이터를 꺼내 작은 파일 조각(json.gz)으로 나눠 RustFS(S3 호환 저장소)에 쌓는 Airflow 작업.**

```
[Oracle SH 스키마] --SQL, 1만 줄씩 fetchmany--> [DAG pipeline_oracle_to_s3] --json.gz--> [RustFS 버킷 oracle-raw]
```

- Spark 는 쓰지 않는다 (메모리 문제). 한 번에 `chunk_size`(10000)행만 메모리에 올린다.
- 같은 DAG 를 다시 실행해도 결과가 중복되지 않는다 (기존 파일 삭제 후 적재).

## 2. 파일 구성 (`airflow/dags/`)

| 파일 | 역할 |
|---|---|
| `pipeline_oracle_to_s3.py` | DAG 정의 (태스크 연결만) |
| `pipeline_config.yaml` | 공통 설정: 원천/타겟, 경로·파일명 템플릿, 삭제 정책, 통계, translog |
| `oracle_table.xml` | 테이블별 설정: SQL, 적재 유형, 기간, 합계 컬럼 |
| `common/config.py` | YAML/XML 읽기와 검증 |
| `common/etl_utils.py` | 실제 적재 로직 (어댑터, 경로·파일명, 기준일 계산, 직렬화, 통계, translog) |
| `_backup_before_leader/` | 리드 소스 적용 전 우리 구현 백업. DAG 파싱 제외 (`.airflowignore`) |

새 테이블은 `oracle_table.xml` 에 `<table>` 을 추가한다. 코드는 고치지 않는다.

## 3. DAG 흐름

```
prepare ─────────┬─> build_data_items ─> load_table ────┐
                 │                                       ├─> write_translog
                 └──────────────────────┐                │
build_meta_items ───────────────────────┴─> load_metadata ┘
(prepare 의 결과는 load_table, load_metadata 에도 전달된다. build_meta_items 는 prepare 와 독립이다)
```

| 태스크 | 하는 일 |
|---|---|
| `prepare` | JobID, generation_ts(KST, DAG 실행당 1회), run_date(오늘, KST)를 만들어 공유 |
| `build_meta_items` | XML `<metadata>` 의 뷰 목록 (9개) |
| `build_data_items` | `<data>` 에서 (테이블, 기준일) 조합을 펼침. FULL 은 1건, PERIOD 는 기준일마다 1건 |
| `load_metadata` / `load_table` | 항목마다 태스크 1개, 동시 실행 `max_parallel`(5). 조회 -> 분할 -> 업로드 -> (data 만) 통계 |
| `write_translog` | 모든 결과를 모아 DAG 실행당 파일 1개로 기록. 실패가 1건이라도 있으면 DAG 실패 |

### 적재 함수의 동작 (`export_query`)
1. SQL 실행 -> `fetchmany(chunk_size)` 로 **첫 청크를 읽는다** (SQL 오류가 있으면 여기서 실패).
2. 그 뒤에 대상 경로의 기존 파일을 삭제한다.
3. 청크마다 gzip NDJSON 을 만들어 업로드하고, 같은 순회에서 건수·컬럼별 NULL 수·SUM 을 계산한다.
4. data 는 파일 단위 통계를 log 경로에 추가로 저장한다.

### FULL 과 PERIOD
| 구분 | 의미 | 예 |
|---|---|---|
| FULL | 표 전체를 한 번에 | SH_PROMOTIONS |
| PERIOD | 최근 N 일/월/년을 기준일별로 따로 조회 | SH_COSTS(`3D`), SH_COSTS_MM(`3M`), SH_COSTS_YYYY(`3Y`) |

| 단위 | 오늘이 2026-10-29 일 때 기준일 | SQL 의 `:BASE_DATE` 형식 |
|---|---|---|
| `3D` | 20261027, 20261028, 20261029 | YYYYMMDD |
| `3M` | 202608, 202609, 202610 | YYYYMM |
| `3Y` | 2024, 2025, 2026 | YYYY |

`:BASE_DATE` 는 Oracle 바인드 변수(빈칸)이고, 코드가 `{"BASE_DATE": 값}` 으로 채워 보낸다.
COSTS 계열 SQL 은 `TIME_ID + (365*4)` 로 날짜를 약 4년 뒤로 밀어 비교한다 (샘플 데이터 2019~2022 를 오늘 날짜 근처로 쓰기 위한 것으로 보임).

## 4. 결과물 위치

버킷 `oracle-raw` 아래, **경로와 파일명은 소문자**:

```
oracle/
├── data/sh/promotions/sh_promotions_{generation_ts}_00000001.json.gz
├── data/sh/costs/20261027/sh_costs_{generation_ts}_20261027_00000001.json.gz
├── log/sh/costs/20261027/...                 data 와 같은 파일명의 통계 파일
├── meta/all_tables/{run_date}/all_tables_{generation_ts}_00000001.json.gz
└── translog/{success|fail}/{run_date}/oracle_translog_{generation_ts}_00000001.json.gz
```

| 종류 | 내용 | 재실행 시 |
|---|---|---|
| data | 원천 데이터 (NDJSON, gzip) | 삭제 후 적재 |
| log | 파일마다 `record_count`, `null_count`, `sum` | 삭제 후 적재 |
| meta | Oracle 구조 정보 9종 (owner = SH) | 삭제 후 적재 |
| translog | DAG 실행당 파일 1개 (`common` 요약 + `items` 배열) | **유지** |

JobID 는 파일명이 아니라 log/translog 파일 **내용**에 들어간다.

## 5. 시간대
컨테이너 `TZ` 와 Airflow `DEFAULT_TIMEZONE` 은 KST, 코드의 시각도 `defaults.timezone`(Asia/Seoul) 이다. 예외는 Airflow 메타DB 내부 저장값(UTC), Oracle `DBTIMEZONE`(+00:00), RustFS 객체 LastModified(UTC).

## 6. 실행 방법

**사전 준비 (Airflow UI > Admin > Connections)**
- `oracle_default`: Oracle 접속 정보
- `rustfs`: 로그인=access key, 비밀번호=secret key, Extra 에 `{"endpoint_url": "http://rustfs:9000"}`

**실행**: `http://localhost:8081` -> DAG `pipeline_oracle_to_s3` -> Trigger
- 파라미터 `base_date_from` / `base_date_to` 를 **둘 다 비우면** 오늘 기준 `auto-load-range` 로 적재한다.
- 과거 구간을 다시 받을 때는 **둘 다** `YYYYMMDD` 로 입력한다 (하나만 넣으면 오류). 예: `20261001` ~ `20261003`.
- `meta` 와 `translog` 경로의 날짜(`run_date`)는 항상 실행 당일이다.

CLI 로 한 번 돌려 볼 때 (RustFS 에 실제로 파일을 쓰고 기존 파일을 삭제한다):
```powershell
docker exec airflow airflow dags test pipeline_oracle_to_s3
```

**확인**: RustFS 콘솔 `http://localhost:9001` 의 `oracle-raw/oracle/`, 실패 시 `translog/fail/` 의 `items[].error`.

## 7. 실패 처리
- 적재 함수는 예외를 던지지 않고 `status=FAILED` 와 오류 메시지가 든 결과를 돌려준다. 그래서 **개별 태스크는 초록색(성공)으로 보여도** 실제로는 실패일 수 있다. `write_translog` 태스크의 결과와 translog 파일을 봐야 한다.
- 실패가 1건이라도 있으면 마지막 `write_translog` 가 DAG 를 실패 처리한다.
- 재시도(`retries`)와 타임아웃은 설정하지 않았다 (리드 원본 기준).

## 8. 검증 현황
| 항목 | 상태 |
|---|---|
| DAG 등록, import 오류 없음 | 확인함 |
| YAML/XML 로딩과 설정 검증, 항목(data 4, metadata 9) | 확인함 |
| 기준일 계산 (`3D`, `3M`, `3Y`, 수동 범위) | 확인함 |
| 경로·파일명 (소문자, generation_ts) | 확인함 |
| RAW(bytes) -> hex 변환, NULL/SUM 통계 | 확인함 (단위 확인) |
| Oracle: `ALL_TAB_PARTITIONS` 컬럼 존재, COSTS 날짜 이동 SQL 의 오늘 기준 3일 조회(150/75/52건) | 확인함 (읽기 전용) |
| **DAG 전체 실행, RustFS 업로드, translog 내용** | **아직 실행 안 함** |
| `S3Hook` + path-style 설정으로 RustFS 접속 | 실행 전이라 미확인 |

## 9. 알려진 제한과 미확정
- 이전 구현(`oracle_sh_to_rustfs`)이 `oracle-raw/oracle_sh/` 에 남긴 파일은 이 DAG 가 지우지 않는다 (경로가 `oracle/` 로 다름).
- 원천은 Oracle, 타겟은 S3(RustFS) 위주로 설정했다. 코드는 PostgreSQL/MySQL/MSSQL/Snowflake 원천과 Snowflake Stage 타겟 어댑터도 가지고 있으나 이 환경에서는 쓰지 않는다.
- PostgreSQL meta 매핑이 없다. `ALL_TAB_PARTITIONS` 조회에는 owner 필터가 없다.
- 설계 문서와 다른 점(JobID, `T`, 소문자, translog 형태)은 `CLAUDE.md` 의 "Design spec 상태" 참고.
