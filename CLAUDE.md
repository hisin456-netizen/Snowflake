# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A local data-pipeline lab: Airflow 3 DAGs that extract from Oracle (Oracle Sample Schemas, e.g. `SH`) and land files in RustFS (S3-compatible object storage). There is no build, lint, or test tooling; the "code" is DAG files plus a Docker Compose stack. Not a git repository. Comments and docs are written in Korean.

## Architecture (데이터 파이프라인 설계)

흐름: Oracle -> Airflow DAG (SQL, 청크 단위 조회) -> json.gz 파일 -> RustFS (S3)

### 구성 (`airflow/dags/`)
구현은 리드 개발자 소스(`leader_developer_source/`)를 기준으로 한다. 분석은 `.claude/docs/reference/leader-developer-source-analysis.md`.
- `pipeline_oracle_to_s3.py`: DAG (dag_id `pipeline_oracle_to_s3`, 수동 실행)
- `pipeline_config.yaml`: 공통 설정 (원천/타겟, 경로·파일명 템플릿, 삭제 정책, 통계, translog)
- `oracle_table.xml`: 테이블별 SQL, 적재 유형, 기간, 합계 컬럼 (data 4개, metadata 9개)
- `common/config.py`, `common/etl_utils.py`: 설정 로딩·검증과 적재 로직
- `_backup_before_leader/`: 리드 소스 적용 전 우리 구현 백업 (`.airflowignore` 로 DAG 파싱 제외)

### 설계 결정
- 추출과 적재는 Airflow DAG 안에서 SQL로 직접 수행한다. DB-API 커서 `fetchmany(chunk_size)` 로 읽어 part 파일로 바로 쓴다.
- 저장 포맷은 json.gz(NDJSON), `chunk_size`(10000)행 단위 part 파일로 분할한다.
- 경로: `{bucket}/{source}/{data|log|meta|translog}/...`. 템플릿은 yaml 의 `storage.paths`, **S3 키는 소문자**로 통일한다.
- `FULL`은 마스터 데이터, `PERIOD`는 트랜잭션 데이터. 기간은 `auto-load-range` (`3D`/`3M`/`3Y`), 기준일은 각각 `YYYYMMDD`/`YYYYMM`/`YYYY` 이고 SQL 의 `:BASE_DATE` 에 바인드된다. 수동 재처리는 DAG 파라미터 `base_date_from`/`base_date_to`.
- 파일명(yaml `storage.filenames`): `{schema}_{table}_{generation_ts}[_{base_date}]_{part_no}.json.gz`. generation_ts 는 KST `%Y%m%d%H%M%S`, DAG 실행당 1회. JobID 는 파일명이 아니라 log/translog 내용에 들어간다.
- 재실행: **첫 청크 조회가 성공한 뒤** data/log/meta 대상 경로의 기존 파일을 삭제하고 적재한다. `translog`는 유지.
- 실패 처리: 적재 함수는 예외를 던지지 않고 FAILED 결과를 반환한다. 마지막 `write_translog` 가 DAG 실행당 파일 1개로 기록(전부 성공 `success/`, 1건이라도 실패 `fail/`)하고, 실패가 있으면 DAG 를 실패 처리한다.
- 리드 원본과 다른 로컬 수정: target `connection_id` 는 `rustfs`, bucket 은 `oracle-raw`, `S3Hook` 에 path-style 설정 추가.

### 하지 않는 것
- Spark는 사용하지 않는다 (메모리 문제). 관련 컨테이너와 코드는 제거되었으므로 되살리지 않는다.
- 전체 테이블 통계는 계산하지 않는다 (log는 파일 단위 통계만).

### 상세 스펙
설계 문서는 `.claude/docs/CreateDag.md` (리드 소스에 맞춰 수정됨). 미확정 항목은 아래 "Design spec 상태" 참고.

## Stack (`docker-compose.yml`, run from the repo root `D:\projects\airflow-lab`)

All services share the `airflow-lab` bridge network and reach each other by container/service name.

| Service | Container | Host port | Notes |
|---|---|---|---|
| airflow | `airflow` | **8081** -> 8080 | `apache/airflow:3.3.2`, LocalExecutor, `airflow standalone` (api-server, scheduler, dag-processor in one container), SimpleAuthManager (password is printed in `docker logs airflow`) |
| postgres | `airflow-postgres` | 5432 | Airflow metadata DB; `restart: "no"` |
| oracle | `oracle-free` | 1521 | `database/free:latest-lite`, service `FREEPDB1`, 비밀번호는 `.env`의 `ORACLE_PWD`; mounts `oracle/sample-schemas` |
| rustfs | `rustfs` | 9000 (S3 API), 9001 (console) | access/secret key는 `.env`; data in `rustfs/data` |

Commands:

```powershell
docker compose up -d postgres      # postgres must be up first; it does NOT auto-restart
docker compose up -d airflow       # or `docker compose up -d` for everything (depends_on waits for postgres health)
docker compose restart airflow     # after postgres was restarted
docker logs airflow --tail 50
docker exec airflow airflow dags test <dag_id>   # run a DAG once without the scheduler
```

If the Airflow UI is unreachable, check `docker ps -a` first: the usual cause is `airflow-postgres` being exited (Airflow then fails with `could not translate host name "postgres"`). The user starts the stack manually and does not want `restart:` policies changed in the compose file.

The Airflow providers (`amazon`, `oracle`) are installed at container start through `_PIP_ADDITIONAL_REQUIREMENTS`, so first boot after a recreate is slow (a minute or two before port 8081 answers).

## Layout

- `airflow/dags/` mounted at `/opt/airflow/dags` (also `airflow/logs`, `airflow/plugins`). Edits are picked up by the dag-processor without a restart.
  - `pipeline_oracle_to_s3.py` (+ `pipeline_config.yaml`, `oracle_table.xml`, `common/`) is the working config-driven Oracle -> RustFS DAG (see Architecture). `oracle_to_rustfs.py` is an old prototype; `oracle_test.py` is a connectivity check; `digdag.py` is an unreferenced `S3TargetAdapter`-style helper draft (not part of the working DAG); `hello_dag.py`, `tutorial.py`, `bash_operator.py` are tutorial samples.
- `snowflake/` Snowflake 쪽 SQL (Airflow 마운트 밖, 실행은 IDE 확장 또는 `snow sql -f` 로 사용자가 한다. MCP 롤은 읽기 전용).
  - `ddl/` 테이블 DDL, `procedures/` 메타 적재 프로시저(`SP_DW_DW_META_*`, 견본 `EG_SQ.sql`), `utils/` 공통 함수·로그 프로시저(`UTILS.sql` 등), `setup/` 초기 설정·파이프, `scratch/` 작업 노트/견본(실행 대상 아님).
  - `procedures/` 의 6개(ALL_TABLES, ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_COL_COMMENTS)는 INFER_SCHEMA 기준으로 생성했고 Snowflake 에서 실행해 검증하지 않았다. ALL_TAB_PARTITIONS 는 추출 SQL 에 PARTITION_NAME 이 없어 보류(SH 만 적재 방침).
- `dev/` 개발 중 생긴 부산물: `python/`(SDK 샘플), `oracle/`(데이터 확인 SQL), `query-results/`(IDE 쿼리 결과), `logs/`, `samples/`. 설계 기준이 아니다.
- `oracle/sample-schemas/` Oracle sample schema SQL and READMEs (vendor content; `oracle/init` is mounted as the DB init dir and is currently empty).
- `docker-compose.old.yml` is a previous compose file; ignore it.

## Airflow conventions used here

- Airflow 3 APIs: `from airflow.sdk import dag, task` (not `airflow.decorators`), `@dag`/`@task` style, `schedule=None`, `catchup=False`, `start_date`는 KST tz-aware (`pipeline_oracle_to_s3.py`는 `datetime(2026, 1, 1, tzinfo=ZoneInfo(...))`).
- Connections are created in the Airflow UI, not in code (see "Connections" below). Use the container hostnames (`oracle-free`, `rustfs`), not `localhost`.
- RustFS requires path-style S3 addressing (`Config(s3={"addressing_style": "path"})` with boto3).
- Oracle reads go through `OracleHook.get_df_by_chunks(sql, chunksize=...)` to bound memory; column names are lower-cased before writing.
- 시간대: 전 시스템 KST(`Asia/Seoul`)로 통일한다. compose의 각 서비스 `TZ`와 Airflow `DEFAULT_TIMEZONE`이 KST이고, 코드의 시각은 `pipeline_config.yaml`의 `defaults.timezone`(`ZoneInfo`)으로 만든다 (`etl_utils.py`, DAG 모두 이 값을 참조). Airflow 메타DB 내부 저장값과 Oracle `DBTIMEZONE`은 UTC 그대로다. 컨테이너 재생성 후 적용된다.

## Connections

Airflow UI(Admin > Connections)에서 생성한다. 비밀값(비밀번호, 키)은 이 문서에 적지 않고 루트의 `.env`(compose가 자동 로드)에 있다. `.env`는 공유/커밋하지 않는다.

| Connection ID | 대상 | 호스트 / 설정 | 사용처 |
|---|---|---|---|
| `oracle_default` | Oracle | `oracle-free:1521`, service `FREEPDB1` | `pipeline_oracle_to_s3`, `oracle_test` |
| `rustfs` | RustFS (S3) | login/password = access/secret key, `extra` JSON에 `endpoint_url` (`http://rustfs:9000`) 필수, `region_name` 선택 | S3 업로드 (`pipeline_oracle_to_s3`) |

## Claude 설정 (`.claude/`)

- `settings.json`: 읽기 전용 docker/airflow 명령 허용, 볼륨 삭제 명령(`down -v`, `volume prune` 등) 차단, `docker compose down`은 확인 요청, `docker-compose.old.yml`과 `oracle/sample-schemas/**` 수정 금지. PostToolUse 훅 연결.
- `skills/agents-airflow-dag-patterns`: Airflow DAG 패턴 스킬 (프로젝트 로컬).
- `rules/testing.md`: DAG 변경 후 검증 절차와 보고 원칙 (테스트 도구가 없으므로 `dags test` 중심).
- `rules/security.md`: 자격증명 하드코딩 금지, 데이터 볼륨 보호, compose 설정 변경 금지.
- `hooks/format-edits.sh`: `airflow/dags/*.py` 수정 후 컨테이너에서 문법 검사 (포매터는 없음).
- `docs/CreateDag.md`: 범용 config 기반 DAG의 상세 설계 스펙 (파일명 규칙, 출력 4종, 재실행 정책). 설계 기준 문서.
- `docs/design-done/`: 구현 완료된 내용의 설명 문서 (코드 흐름, 실행 방법, 검증 현황). 설계 기준(`CreateDag.md`)과 구분해 둔다.
- `docs/reference/`: 참고용 자료. 리드 개발자 소스(`leader_developer_source/`) 분석 결과가 있다. 원본 소스는 수정하지 않는다.

## Design spec 상태 (`.claude/docs/CreateDag.md`)

`CreateDag.md` 는 리드 개발자 소스에 맞춰 수정했고 구현과 일치한다 (수정 전 원본은 `.claude/docs/reference/CreateDag.before-leader.md`). 구현과 문서가 다르게 보이면 리드 소스(`leader_developer_source/`) 기준이다.

미확정 항목 (코드에 반영하기 전에 사용자에게 확인):
- PostgreSQL용 meta 매핑 없음
- `ALL_TAB_PARTITIONS` 조회에 owner 필터 없음
- translog 후속 작업(Snowpipe 트리거) 방식
- `SH_COSTS_MM`(3M), `SH_COSTS_YYYY`(3Y) 항목이 시험용인지 운영 대상인지
