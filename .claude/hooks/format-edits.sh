#!/bin/bash
# PostToolUse(Edit|Write): 수정된 DAG 파일 사후 점검.
# 이 프로젝트에는 포매터/린터가 없으므로 자동 포맷은 하지 않고,
# airflow/dags/*.py 의 문법 오류만 컨테이너 안에서 확인한다.
# 컨테이너가 꺼져 있으면 건너뜀. exit 2 = 오류를 Claude에게 전달.
input=$(cat)

path=$(echo "$input" | sed -n 's/.*"file_path"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | sed 's#\\\\#/#g')

case "$path" in
  */airflow/dags/*.py) ;;
  *) exit 0 ;;
esac

rel="${path#*airflow/dags/}"

docker ps --format '{{.Names}}' 2>/dev/null | grep -qx airflow || exit 0

out=$(docker exec airflow python -c "import ast; ast.parse(open('/opt/airflow/dags/$rel', encoding='utf-8').read())" 2>&1)
if [ $? -ne 0 ]; then
  echo "DAG 문법 오류 ($rel):" >&2
  echo "$out" >&2
  exit 2
fi
exit 0
