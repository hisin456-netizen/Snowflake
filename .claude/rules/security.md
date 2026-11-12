# 보안 / 안전 규칙

## 자격증명
- 비밀번호와 키(`Oracle1234`, `rustfsadmin1234` 등)를 새 코드·DAG·스크립트에 하드코딩하지 않는다.
- DAG는 Airflow Connection(`oracle_default`, `rustfs`)을 사용한다.
- 로그·응답·문서 예시에 실제 키를 새로 복사해 넣지 않는다.
- 비밀값은 루트 `.env`에 둔다 (compose가 `${VAR}`로 참조). `.env`는 git에 올리거나 공유하지 않으며, git 도입 시 `.gitignore`에 넣는다.

## 데이터 보호
- `rustfs/data`, `oracle` 볼륨은 삭제하지 않는다. `down -v`, `volume prune`, `system prune`은 settings.json에서 차단되어 있다.
- `docker compose down` / `rm`은 실행 전 사용자 확인을 거친다.
- `docker-compose.old.yml`과 `oracle/sample-schemas/**`(벤더 콘텐츠)는 수정하지 않는다.

## 인프라 설정
- compose의 `restart:` 정책은 사용자가 의도한 값이므로 바꾸지 않는다.
- 호스트 포트(8081, 5432, 1521, 9000, 9001)를 임의로 변경·추가하지 않는다.

## SQL
- DB 조회에 외부 값을 넣을 때는 문자열 포매팅 대신 바인드 변수(`:BASE_DATE` 등)를 사용한다.
- 설계 스펙의 메타 쿼리에는 owner 필터가 없다. 코드에 반영하기 전에 사용자에게 확인한다.
