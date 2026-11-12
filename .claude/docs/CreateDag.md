#. airflow dag 생성

> 이 문서는 리드 개발자 소스(`leader_developer_source/`)에 맞춰 수정했다. 수정 전 원본은 `reference/CreateDag.before-leader.md`.
> 구현 코드는 `airflow/dags/` 의 `pipeline_oracle_to_s3.py`, `pipeline_config.yaml`, `oracle_table.xml`, `common/` 이다.

1. 원천 정의 : 설정파일에서 원천 유형 정의
   설정은 두 파일로 나눈다.

- `pipeline_config.yaml` : 공통 설정 (source, target, defaults, storage, statistics, translog)
- `oracle_table.xml` : 테이블 설정 (`<data>` 데이터 테이블, `<metadata>` 스키마 정보 뷰). 항목마다 id, schema, name, load-type, auto-load-range, sum-columns, sql
- 원천 추출 유형: SQL기반의 DB와 REST_API 기반
- 대상(`source.system_type`): ORACLE, POSTGRESQL, MYSQL, MSSQL, SNOWFLAKE. 현재 사용: ORACLE
- 비고: REST_API 기반은 추후 정의

2. target 정의 : `pipeline_config.yaml` 의 `target` 에서 타겟 유형 정의

- Blob Storage : AWS S3, Google Cloud Storage, Azure Blob Storage, Snowflake Internal Stage
- 대상(`target.type`): S3 계열(S3, S3_COMPATIBLE, RUSTFS, MINIO), SNOWFLAKE_STAGE. 현재 사용: RustFS S3
- 비고: Google Cloud Storage, Azure Blob Storage 는 코드에 아직 없음

3. 데이터 조회 방식 : `oracle_table.xml` 의 `load-type` 에서 조회 유형 정의

- 전체 조회(FULL) : master data
- 기준일자 기반 조회(PERIOD) : transaction data. `auto-load-range` 로 기간 지정 (3D, 3M, 3Y)

4. 저장 방식 : `pipeline_config.yaml` 의 `defaults`, `storage`, `statistics`, `translog` 에서 정의
   4.1 개요

- data, log, meta, translog 구분으로 저장
- data : 원천 데이터
- log : 검증을 위한 조회된 원천 데이터의 통계 정보. 분할 파일별 조회 결과. 컬럼별 통계. (건수, null 카운트, sum)
  레코드 건수(record_count): 파일별 저장 레코드 수
  NULL 카운트(null_count): 컬럼별 NULL 값 개수
  SUM(sum): 설정된 숫자형 컬럼의 합계. 정밀도를 위해 문자열로 저장
  통계 결과: data 파일과 동일한 JobID, 기준일자, PartNo로 연결 (log 파일 안에 job_id, base_date 와 파일별 part_no 기록)

  ## 테이블 전체 통계는 산출하지 않는다.(시스템 죽어요)

- meta : 원천 데이터 스키마 정보. (테이블, 컬럼, PK, index, 코멘트, 파티션 정보)
- translog : dag에서의 조회 및 target 적재 성공/실패 등 정보. DAG 실행당 파일 1개. 후속 작업(snowflake snowpipe)의 시작점은 추후 정의
  - 구조: `common`(job_id, dag_id, run_id, source, run_date, generation_ts, run_status, total, success, failed, failed_items) + `items`(테이블·기준일별 category, table_id, schema, table, load_type, base_date, status, file_count, record_count, deleted_count, 경로, started_at, ended_at, elapsed_sec, error)
  - status 는 SUCCESS / FAILED
- 파일 데이터 포맷 : json.gz (gzip, UTF-8)
  - data, meta : 한 줄에 레코드 1건인 JSON Lines
  - log, translog : JSON 문서 1건
  - 객체에 `Content-Type: application/json`, `Content-Encoding: gzip` 헤더를 설정

  4.2 경로
  경로는 `storage.paths` 템플릿으로 정의하며 `{root}` 는 버킷이다. **S3 경로와 파일명은 소문자로 통일**한다. (파일 내용의 컬럼명 등은 변경하지 않는다)
  4.2.1 data 경로

- 전체 유형: {버킷명}/{원천명}/data/{스키마명 또는 DB명}/{테이블명}
- 기간 유형: {버킷명}/{원천명}/data/{스키마명 또는 DB명}/{테이블명}/{기준일자}
- 기준일자 형식: D 단위 YYYYMMDD, M 단위 YYYYMM, Y 단위 YYYY

  4.2.2 log 경로

- 전체 유형: {버킷명}/{원천명}/log/{스키마명 또는 DB명}/{테이블명}
- 기간 유형: {버킷명}/{원천명}/log/{스키마명 또는 DB명}/{테이블명}/{기준일자}

  4.2.3 meta 경로

- 전체 유형: {버킷명}/{원천명}/meta/{스키마 정보 테이블명}/{실행일자-YYYYMMDD}
- oracle 기준 스키마 정보 테이블명 : ALL_TABLES, ALL_TAB_COLUMNS, ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_TAB_COMMENTS, ALL_COL_COMMENTS, ALL_TAB_PARTITIONS

  4.2.4 translog 경로

- 전체 유형 성공: {버킷명}/{원천명}/translog/success/{실행일자-YYYYMMDD}
- 전체 유형 실패: {버킷명}/{원천명}/translog/fail/{실행일자-YYYYMMDD}
- 전체 적재 항목이 모두 성공이면 success, 1건이라도 실패면 fail 경로에 기록

  4.3 파일명

- 적재 성능을 위한 분할 적재. 분할 건수는 `defaults.chunk_size` 에서 정의. 예시) 10000건
- JobID : Airflow DAG 실행을 식별하는 고유 ID. Airflow run_id 기반, 파일명에 쓸 수 없는 문자는 `_` 로 변환. **파일명에는 사용하지 않고** log, translog 내용에 기록
- PartNo : 분할된 파일의 인덱스번호. 00000001부터 시작하는 8자리 숫자
- Generation timestamp : KST 기반 YYYYMMDDHHMMSS (`defaults.timezone` = Asia/Seoul). Dag당 1회 생성 사용
- 기준일자: 조회 처리 기준일자. D 단위 YYYYMMDD, M 단위 YYYYMM, Y 단위 YYYY
- 전체 유형: {스키마명 또는 DB명}_{테이블명}_{Generation timestamp}_{PartNo}.json.gz
- 기간 유형: {스키마명 또는 DB명}_{테이블명}_{Generation timestamp}_{기준일자}\_{PartNo}.json.gz
- meta 유형: {스키마 정보 테이블명}_{Generation timestamp}_{PartNo}.json.gz
- translog 유형: {원천명}_translog_{Generation timestamp}_{PartNo}.json.gz
- data와 log는 각각 전체/기간 유형으로 적용 (log 는 data 와 같은 파일명 규칙)
- meta와 translog는 전체 유형으로 적용

  4.4 삭제 규칙

- data, log, meta는 각 정의 된 경로에 기존 파일이 있을 경우 삭제 후 적재 (`DELETE_EXISTING_FILES_BEFORE_LOAD`)
  - data, meta 는 **첫 청크 조회가 성공한 뒤에** 삭제한다. SQL 오류로 기존 적재분이 사라지지 않게 하기 위함
  - 삭제 대상은 경로 뒤에 `/` 를 붙여 조회하므로 `sh/costs` 삭제가 `sh/costs2` 를 건드리지 않는다
- translog는 삭제 없음 (`KEEP_EXISTING_FILES`)

5. 기타
   5.1 airflow connection 정보 사전 등록

- oracle (`oracle_default`), s3 (`rustfs`, Extra 에 `endpoint_url` 필요)
- Connection ID 와 버킷은 `pipeline_config.yaml` 에서 지정

  5.2 데이터 조회 sql 예시

- auto-load-range=3D: 실행 기준일을 포함한 최근 3개 일자를 처리 (3M: 당월 포함 3개월, 3Y: 당년 포함 3년)
- BASE_DATE: 개별 조회 및 저장에 사용하는 날짜. SQL 에서 사용할 수 있는 유일한 바인드 변수
- 각 날짜별로 별도 SQL 조회 및 파일 생성
- 실행일이 2026-10-22라면, 기준일은 20261020, 20261021, 20261022
- 수동 재처리: DAG 파라미터 `base_date_from`, `base_date_to` (YYYYMMDD)를 **둘 다** 입력하면 그 범위를 단위(D/M/Y)에 맞춰 처리. 둘 다 비우면 auto-load-range 사용
- 개발 환경의 샘플 데이터(2019~2022)는 `TIME_ID + (365*4)` 로 날짜를 이동해 조회한다 (`oracle_table.xml`)

          <table>
              <id>SH_PROMOTIONS</id>
              <schema>SH</schema>
              <name>PROMOTIONS</name>
              <load-type>FULL</load-type>
              <auto-load-range/>
              <statistics>
                  <sum-columns/>
              </statistics>
              <sql><![CDATA[
                  SELECT
                      PROMO_ID,
                      PROMO_NAME,
                      PROMO_SUBCATEGORY,
                      PROMO_SUBCATEGORY_ID,
                      PROMO_CATEGORY,
                      PROMO_CATEGORY_ID,
                      PROMO_COST,
                      PROMO_BEGIN_DATE,
                      PROMO_END_DATE,
                      PROMO_TOTAL,
                      PROMO_TOTAL_ID
                  FROM SH.PROMOTIONS
              ]]></sql>
          </table>

          <table>
              <id>SH_COSTS</id>
              <schema>SH</schema>
              <name>COSTS</name>
              <load-type>PERIOD</load-type>
              <auto-load-range>3D</auto-load-range>
              <statistics>
                  <sum-columns>
                      <column>UNIT_COST</column>
                      <column>UNIT_PRICE</column>
                  </sum-columns>
              </statistics>
              <sql><![CDATA[
                  SELECT
                      PROD_ID,
                      TIME_ID,
                      PROMO_ID,
                      CHANNEL_ID,
                      UNIT_COST,
                      UNIT_PRICE
                  FROM SH.COSTS
                  WHERE TIME_ID >= TO_DATE(:BASE_DATE, 'YYYYMMDD')
                    AND TIME_ID <  TO_DATE(:BASE_DATE, 'YYYYMMDD') + 1
              ]]></sql>
          </table>

  5.3 mata 데이터 조회 sql 예시

- 예시는 1개만
- 대상 : ALL_TABLES, ALL_TAB_COLUMNS, ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_TAB_COMMENTS, ALL_COL_COMMENTS, ALL_TAB_PARTITIONS
- `<metadata>` 구획에 작성. FULL 만 지원하고 바인드 변수는 사용할 수 없음
- 소유자(OWNER) 필터는 SQL 에 직접 작성 (ALL_IND_COLUMNS 는 INDEX_OWNER)

          <table>
              <id>ALL_TABLES</id>
              <schema/>
              <name>ALL_TABLES</name>
              <load-type>FULL</load-type>
              <auto-load-range/>
              <statistics>
                  <sum-columns/>
              </statistics>
              <sql><![CDATA[SELECT * FROM ALL_TABLES WHERE OWNER = 'SH']]></sql>
          </table>

6. 처리 흐름과 실패 처리
   6.1 태스크

- prepare : JobID, Generation timestamp, 실행일자 생성 (DAG 실행당 1회)
- build_meta_items / build_data_items : 처리 대상 목록 생성. PERIOD 는 기준일마다 항목 1개
- load_metadata / load_table : 항목마다 태스크 1개. 동시 실행 수는 `defaults.max_parallel`
- write_translog : 모든 결과를 모아 translog 1개 기록 (`trigger_rule=all_done`)

  6.2 실패 처리

- 적재 함수는 예외를 던지지 않고 status=FAILED 와 오류 메시지가 든 결과를 반환한다
- 따라서 개별 태스크는 성공으로 표시되어도 실제로는 실패일 수 있다. 결과는 translog 로 확인한다
- write_translog 에서 실패가 1건이라도 있으면 DAG 를 실패 처리한다
- 태스크가 비정상 종료되어 결과가 없는 항목은 FAILED 로 간주해 translog 에 포함한다

  6.3 설정 검증 (DAG 로딩 시점)

- 지원하지 않는 바인드 변수 (허용: BASE_DATE)
- PERIOD 인데 auto-load-range 가 없거나 단위가 D/M/Y 가 아닌 경우
- data 항목의 schema 가 비어 있는 경우
- metadata 항목이 FULL 이 아니거나 바인드 변수를 쓰는 경우
- (실행 시점) 경로와 파일명에 들어가는 값에 허용되지 않는 문자나 `..` 가 있는 경우

7. 로컬 환경 수정 (리드 소스 원본과 다른 점)

- target `connection_id` : `rustfs`
- target `bucket` : `oracle-raw`
- `S3Hook` 에 RustFS 용 path-style 주소 지정 설정 추가 (`common/etl_utils.py`)

8. 미확정 항목

- PostgreSQL용 meta 매핑
- ALL_TAB_PARTITIONS 조회에 owner 필터 없음
- translog 후속 작업(Snowpipe 트리거) 방식
- `SH_COSTS_MM`(3M), `SH_COSTS_YYYY`(3Y) 항목이 시험용인지 운영 대상인지
