import re
from dataclasses import dataclass
from typing import Self
from urllib.parse import unquote

from s3_parse_url import parse_s3_dsn


@dataclass
class S3Credentials:
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str

    @classmethod
    def parse_dsn(cls, dsn: str) -> Self:
        if not isinstance(dsn, str):
            raise ValueError("Wrong format of S3_DSN")
        if dsn.startswith("minio://"):
            return cls._parse_do_dsn(dsn)
        return cls._parse_s3_dsn(dsn)

    @classmethod
    def _parse_s3_dsn(cls, dsn: str) -> Self:
        s3_credentials = parse_s3_dsn(dsn)
        return cls(
            endpoint_url=unquote(s3_credentials.endpoint_url),
            access_key_id=unquote(s3_credentials.access_key_id),
            secret_access_key=unquote(s3_credentials.secret_access_key),
            bucket_name=unquote(s3_credentials.bucket_name),
        )

    @classmethod
    def _parse_do_dsn(cls, dsn: str) -> Self:
        dsn_pattern = r"^minio://[a-zA-Z0-9_\-]+:[a-zA-Z0-9_\-/]+@[a-zA-Z0-9_\-.:]+(/[a-zA-Z0-9_\-/]*)?$"
        if not re.fullmatch(dsn_pattern, dsn):
            raise ValueError("Wrong format of S3_DSN")

        value = dsn.removeprefix("minio://")
        netloc, path = value.rsplit("@", 1)
        user, password = netloc.split(":", 1)
        endpoint_url, bucket_name = path.split("/", 1)

        return cls(
            endpoint_url=unquote("http://" + endpoint_url),
            access_key_id=unquote(user),
            secret_access_key=unquote(password),
            bucket_name=unquote(bucket_name),
        )
