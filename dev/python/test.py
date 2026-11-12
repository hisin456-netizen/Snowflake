# ======================================================================
# Snowpipe REST API 샘플 (snowflake-ingest SDK)
#
# 하는 일: 스테이지에 이미 올라가 있는 파일 목록을 파이프에 알려서(insertFiles)
#          Snowflake 가 테이블로 적재하게 하고, 적재 결과를 조회한다.
# 주의: 이 코드는 파일을 직접 읽거나 올리지 않는다. "적재해 달라"는 요청만 보낸다.
#       <...> 로 표시된 곳은 실제 값으로 바꿔야 실행된다 (키/계정 값은 코드에 직접 쓰지 말 것).
# 사전 준비: pip install snowflake-ingest   (import 는 설치를 해주지 않는다)
# ======================================================================

# --- [1] import --------------------------------------------------------
from logging import getLogger
from snowflake.ingest import SimpleIngestManager   # 핵심: REST 요청을 대신 보내 주는 클라이언트
from snowflake.ingest import StagedFile            # 적재할 파일 한 개(경로, 크기)를 감싸는 클래스
from snowflake.ingest.utils.uris import DEFAULT_SCHEME
from datetime import timedelta
from requests import HTTPError                     # HTTP 요청 실패 시 발생하는 예외
# 아래는 비공개키(.p8) 파일을 읽어서 SDK 가 쓸 수 있는 문자열로 바꾸는 데 쓴다
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import time
import datetime
import os
import logging

# --- [2] 로그 설정 -----------------------------------------------------
# DEBUG 레벨로 파일에 기록한다. /tmp 는 리눅스 경로라 Windows 에서는 다른 경로로 바꿔야 한다.
logging.basicConfig(
        filename='/tmp/ingest.log',
        level=logging.DEBUG)
logger = getLogger(__name__)

# --- [3] 비공개키 읽기 -------------------------------------------------
# 비공개키를 암호(passphrase)로 보호했다면 그 암호를 돌려주는 함수를 구현한다.
# (암호 없이 만든 키라면 아래 load_pem_private_key 의 password 를 None 으로 둔다)
def get_private_key_passphrase():
  return '<private_key_passphrase>'
#   암호화된 키가 생성되면, get_private_key_passphrase() 함수를 구현하여 해당 키의 암호를 해독하기 위한 암호 구문을 반환합니다.

# rsa_key.p8 파일(비공개키)을 바이트로 읽어서 키 객체로 변환한다.
# 짝이 되는 "공개키"는 미리 Snowflake 사용자에 등록해 두어야 한다 (ALTER USER ... SET RSA_PUBLIC_KEY).
with open("/<private_key_path>/rsa_key.p8", 'rb') as pem_in:
  pemlines = pem_in.read()
  private_key_obj = load_pem_private_key(pemlines,
  get_private_key_passphrase().encode(),
  default_backend())

# SDK 는 암호 없는 PEM 문자열을 요구하므로, 키 객체를 PEM 텍스트로 다시 변환한다.
private_key_text = private_key_obj.private_bytes(
  Encoding.PEM, PrivateFormat.PKCS8, NoEncryption()).decode('utf-8')
# Assume the public key has been registered in Snowflake:
# private key in PEM format

# --- [4] 클라이언트 생성 -----------------------------------------------
# "누가(user + 키) / 어느 계정(account, host)의 / 어느 파이프(pipe)에" 요청할지 정한다.
# SDK 가 이 정보로 JWT 토큰을 만들어 서명하고 URL 을 조립한다.
ingest_manager = SimpleIngestManager(account='<account_identifier>',
                                     host='<account_identifier>.snowflakecomputing.com',
                                     user='<user_login_name>',
                                     pipe='<db_name>.<schema_name>.<pipe_name>',   # DB.SCHEMA.PIPE 전체 이름
                                     private_key=private_key_text)

# --- [5] 적재할 파일 목록 ----------------------------------------------
# 경로는 "파이프 COPY 문의 FROM 경로 기준 상대 경로"여야 한다.
#   예) FROM @스테이지/oracle/data/sh/costs/ 인 파이프라면 '20261102/xxx.json.gz'
# 파일 크기는 선택이지만 권장한다 (모르면 None).
# List of files, but wrapped into a class
staged_file_list = [
  StagedFile('<path>/<filename>', <file_size_in_bytes>),  # file size is optional but recommended, pass None if not available
  StagedFile('<path>/<filename>', <file_size_in_bytes>),  # file size is optional but recommended, pass None if not available
  ...
  ]

# --- [6] insertFiles 요청 ----------------------------------------------
# 한 번에 최대 5,000 개까지 보낼 수 있다 (공식 문서 기준).
try:
    resp = ingest_manager.ingest_files(staged_file_list)
except HTTPError as e:
    # HTTP error, may need to retry
    # 401: 키/토큰 문제, 403: 권한 문제, 429: 요청 과다(나눠서 재시도)
    logger.error(e)
    exit(1)

# This means Snowflake has received file and will start loading
# SUCCESS 는 "요청을 접수했다"는 뜻이다. 적재가 끝났다는 뜻이 아니다.
assert(resp['responseCode'] == 'SUCCESS')

# --- [7] 적재 결과 확인 (폴링) -----------------------------------------
# 적재는 비동기로 진행되므로 결과가 이력에 나타날 때까지 주기적으로 조회한다.
# Needs to wait for a while to get result in history
while True:
    # 최근 일정 시간(문서상 약 10분) 안에 적재되었거나 실패한 파일 목록
    history_resp = ingest_manager.get_history()

    if len(history_resp['files']) > 0:
        print('Ingest Report:\n')
        print(history_resp)
        break          # 결과가 나왔으면 반복 종료
    else:
        # wait for 20 seconds
        time.sleep(20)

    # 시간 범위를 지정해서 이력을 조회한다 (1시간 전부터). 위 break 에 걸리면 여기까지 오지 않는다.
    hour = timedelta(hours=1)
    date = datetime.datetime.utcnow() - hour
    history_range_resp = ingest_manager.get_history_range(date.isoformat() + 'Z')

    print('\nHistory scan report: \n')
    print(history_range_resp)
