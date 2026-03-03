from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Optional

import boto3

from .envelope import decode_envelope, is_lfs_envelope, LfsEnvelope


@dataclass
class ResolvedRecord:
    envelope: Optional[LfsEnvelope]
    payload: bytes
    is_envelope: bool


class LfsResolver:
    def __init__(self, bucket: str, s3_client=None, validate_checksum: bool = True, max_size: int = 0) -> None:
        self.bucket = bucket
        self.s3 = s3_client or boto3.client("s3")
        self.validate_checksum = validate_checksum
        self.max_size = max_size

    def resolve(self, value: bytes) -> ResolvedRecord:
        if not is_lfs_envelope(value):
            return ResolvedRecord(None, value, False)

        env = decode_envelope(value)
        obj = self.s3.get_object(Bucket=self.bucket, Key=env.key)
        payload = obj["Body"].read()
        if self.max_size > 0 and len(payload) > self.max_size:
            raise ValueError("payload exceeds max size")
        if self.validate_checksum:
            expected = env.checksum or env.sha256
            actual = hashlib.sha256(payload).hexdigest()
            if actual != expected:
                raise ValueError("checksum mismatch")
        return ResolvedRecord(env, payload, True)
