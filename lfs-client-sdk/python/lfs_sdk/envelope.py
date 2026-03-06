from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Dict


@dataclass
class LfsEnvelope:
    kfs_lfs: int
    bucket: str
    key: str
    size: int
    sha256: str
    checksum: str | None = None
    checksum_alg: str | None = None
    content_type: str | None = None
    original_headers: Dict[str, str] | None = None
    created_at: str | None = None
    proxy_id: str | None = None


def is_lfs_envelope(value: bytes | None) -> bool:
    if not value or len(value) < 15:
        return False
    if value[:1] != b"{":
        return False
    prefix = value[:50].decode("utf-8", errors="ignore")
    return "\"kfs_lfs\"" in prefix


def decode_envelope(value: bytes) -> LfsEnvelope:
    payload = json.loads(value.decode("utf-8"))
    if not payload.get("kfs_lfs") or not payload.get("bucket") or not payload.get("key") or not payload.get("sha256"):
        raise ValueError("invalid envelope: missing required fields")
    return LfsEnvelope(**payload)
