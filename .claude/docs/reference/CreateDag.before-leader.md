#. airflow dag 생성

1. 원천 정의 : 각 원천별 설정파일에서 원천 유형 정의

- 원천 추출 유형: SQL기반의 DB와 REST_API 기반
- 대상: ORACLE, POSTGRESQL
- 비고: REST_API 기반은 추후 정의

2. target 정의 : 각 원천별 설정파일에서 타겟 유형 정의

- Blob Storage : AWS S3, Google Cloud Storage, Azure Blob Storage, Snowflake Internal Stage
- 대상: Rustsfs S3, Snowflake Internal Stage

3. 데이터 조회 방식 : 각 원천별 설정파일에서 조회 유형 정의

- 전체 조회 : master data
- 기준일자 기반 조회 : transaction data

4. 저장 방식 : 각 원천별 설정파일에서 정의
   4.1 개요

- data, log, meta, translog 구분으로 저장
- data : 원천 데이터
- log : 검증을 위한 조회된 원천 데이터의 통계 정보. 분할 파일별 조회 결과. 컬럼별 통계. (건수, null 카운트, sum)
  레코드 건수: 파일별 저장 레코드 수
  NULL 카운트: 컬럼별 NULL 값 개수
  SUM: 설정된 숫자형 컬럼의 합계
  통계 결과: data 파일과 동일한 JobID, 기준일자, PartNo로 연결

  ## 테이블 전체 통계는 산출하지 않는다.(시스템 죽어요)

- meta : 원천 데이터 스키마 정보. (테이블, 컬럼, PK, index, 코멘트 정보)
- translog : dag에서의 조회 및 target 적재 성공/실패 등 정보. 데이터 내용은 추후 정의. 추후 후속 작업(snowflake snowpipe)의 시작점
- 파일 데이터 포맷 : json.gz

  4.2 경로
  4.2.1 data 경로

- 전체 유형: {버킷명}/{원천명}/data/{스키마명 또는 DB명}/{테이블명}
- 기간 유형: {버킷명}/{원천명}/data/{스키마명 또는 DB명}/{테이블명}/{기준일자-YYYYMMDD}

  4.2.2 log 경로

- 전체 유형: {버킷명}/{원천명}/log/{스키마명 또는 DB명}/{테이블명}
- 기간 유형: {버킷명}/{원천명}/log/{스키마명 또는 DB명}/{테이블명}/{기준일자-YYYYMMDD}

  4.2.3 meta 경로

- 전체 유형: {버킷명}/{원천명}/meta/{스키마 정보 테이블명}/{실행일자-YYYYMMDD}
- oracle 기준 스키마 정보 테이블명 : ALL_TABLES, ALL_TAB_COLUMNS, ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_TAB_COMMENTS, ALL_COL_COMMENTS

  4.2.4 translog 경로

- 전체 유형 성공: {버킷명}/{원천명}/translog/success/{실행일자-YYYYMMDD}
- 전체 유형 실패: {버킷명}/{원천명}/translog/fail/{실행일자-YYYYMMDD}

  4.3 파일명

- 적재 성능을 위한 분할 적재. 분할 건수는 각 원천별 설정파일에서 정의. 예시) 10000건
- JobID : Airflow DAG 실행을 식별하는 고유 ID. Airflow run_id 기반 파일명에 사용할 때 안전하게 변환
- PartNo : 분할된 파일의 인덱스번호. 00000001부터 시작하는 8자리 숫자
- Generation timestamp : KST 기반 YYYYMMDDTHHMMSS. Dag당 1회 생성 사용
- 기준일자: 조회 처리 기준일자 YYYYMMDD
- 전체 유형: {스키마명 또는 DB명}_{테이블명}_{JobID}_{Generation timestamp}_{PartNo}.json.gz
- 기간 유형: {스키마명 또는 DB명}_{테이블명}_{JobID}_{Generation timestamp}_{기준일자}\_{PartNo}.json.gz
- data와 log는 각각 전체/기간 유형으로 적용
- meta와 translog는 전체 유형으로 적용

  4.4 삭제 규칙

- data, log, meta는 각 정의 된 경로에 기존 파일이 있을 경우 삭제 후 적재
- translog는 삭제 없음

5. 기타
   5.1 airflow connection 정보 사전 등록

- oracle, s3

  5.2 데이터 조회 sql 예시

- load-range-day=3: 실행 기준일을 포함한 최근 3개 일자를 처리
- BASE_DATE: 개별 조회 및 저장에 사용하는 날짜
- 각 날짜별로 별도 SQL 조회 및 파일 생성
- 실행일이 2026-10-22라면, 기준일은 20261020, 20261021, 20261022

          <table>
              <schema>SH</schema>
              <name>PROMOTIONS</name>
              <load-type>FULL</load-type>
              <load-range-day/>
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
              <schema>SH</schema>
              <name>COSTS</name>
              <load-type>PERIOD</load-type>
              <load-range-day>3</load-range-day>
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
- 대상 : ALL_TABLES, ALL_TAB_COLUMNS, ALL_CONSTRAINTS, ALL_CONS_COLUMNS, ALL_INDEXES, ALL_IND_COLUMNS, ALL_TAB_COMMENTS, ALL_COL_COMMENTS
  <table>
  <schema>SYS</schema>
  <name>ALL_TABLES</name>
  <load-type>FULL</load-type>
  <load-range-day/>
  <sql><![CDATA[
                SELECT * FROM ALL_TABLES
            ]]></sql>
  </table>
