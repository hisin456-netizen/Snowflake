# 테스트 / 검증 규칙

이 프로젝트에는 단위 테스트나 린트 도구가 없다. 변경 후 검증은 아래 절차로 한다.

## DAG 변경 후
1. `docker ps`로 `airflow` 컨테이너가 떠 있는지 확인 (postgres가 exited면 먼저 복구).
2. `docker exec airflow airflow dags list-import-errors`로 import 오류 확인.
3. `docker exec airflow airflow dags test <dag_id>`로 스케줄러 없이 1회 실행.
4. 실패 시 Connection 누락(`oracle_default`, `rustfs`)과 호스트명(`oracle-free`, `rustfs`)부터 점검.

## 결과 확인
- RustFS 콘솔(http://localhost:9001)에서 `{bucket}/{source}/...` 경로의 파일 생성 여부 확인.

## 보고 원칙
- 실행해 보지 않은 것은 "검증하지 않음"이라고 명시한다.
- 실패한 출력은 숨기지 않고 핵심 오류와 함께 보고한다.
- DAG 재실행은 대상 경로의 기존 data/log/meta 파일을 삭제하므로(설계 스펙 기준) 실행 전 대상 경로를 확인한다.
