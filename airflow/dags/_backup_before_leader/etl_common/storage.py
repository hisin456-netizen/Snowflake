"""RustFS(S3) 접근 헬퍼."""
import gzip
import io
import json

import boto3
from botocore.config import Config
from airflow.sdk import BaseHook


def get_s3(conn_id):
    """Airflow Connection 에서 접속정보를 꺼내 S3 클라이언트 생성"""
    conn = BaseHook.get_connection(conn_id)
    extra = conn.extra_dejson
    return boto3.client(
        "s3",
        endpoint_url=extra["endpoint_url"],
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        region_name=extra.get("region_name", "us-east-1"),
        config=Config(s3={"addressing_style": "path"}),   # RustFS 는 path-style 필수
    )


def ensure_bucket(s3, bucket):
    if bucket not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=bucket)


def delete_prefix(s3, bucket, prefix):
    """기존 파일 삭제 (재실행 시 data/log/meta). prefix 는 '/' 로 끝나야 다른 테이블을 건드리지 않는다."""
    if not prefix.endswith("/"):
        raise ValueError(f"prefix 는 '/' 로 끝나야 합니다: {prefix}")
    deleted = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    return deleted


def put_bytes(s3, bucket, key, body):
    s3.upload_fileobj(io.BytesIO(body), bucket, key)


def _bytes_to_hex(value):
    return value.hex() if isinstance(value, (bytes, bytearray)) else value


def df_to_jsonl_gz(df):
    """DataFrame -> JSON Lines -> gzip. RAW 컬럼(bytes)은 hex 문자열로 저장한다."""
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].map(_bytes_to_hex)
    raw =df.to_json(orient="records", lines=True, date_format="iso", force_ascii=False)
    return gzip.compress((raw + "\n").encode("utf-8"))


def obj_to_json_gz(obj):
    return gzip.compress(json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8"))
