import hashlib
import io
import json
import zlib

import pytest

from lfs_sdk.resolver import LfsResolver

ORIGINAL = bytes([42]) * 262144
CORRUPT = bytes([42 ^ 1]) + ORIGINAL[1:]
CHECKSUMS = {
    "sha256": hashlib.sha256(ORIGINAL).hexdigest(),
    "md5": hashlib.md5(ORIGINAL).hexdigest(),
    "crc32": format(zlib.crc32(ORIGINAL) & 0xFFFFFFFF, "08x"),
    "none": "",
}


class StaticS3:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def get_object(self, Bucket: str, Key: str):
        return {"Body": io.BytesIO(self.payload)}


def envelope(**extra) -> bytes:
    env = {"kfs_lfs": 1, "bucket": "test", "key": "blob", "size": len(ORIGINAL), "sha256": CHECKSUMS["sha256"]}
    env.update(extra)
    return json.dumps(env).encode()


def alg_envelope(alg: str) -> bytes:
    if CHECKSUMS[alg]:
        return envelope(checksum=CHECKSUMS[alg], checksum_alg=alg)
    return envelope(checksum_alg=alg)


@pytest.mark.parametrize("alg", sorted(CHECKSUMS))
def test_valid_object_resolves_exact_bytes(alg):
    result = LfsResolver("test", s3_client=StaticS3(ORIGINAL)).resolve(alg_envelope(alg))
    assert result.is_envelope
    assert result.payload == ORIGINAL


@pytest.mark.parametrize("alg", sorted(CHECKSUMS))
def test_corrupt_object_is_rejected(alg):
    with pytest.raises(ValueError, match="checksum mismatch"):
        LfsResolver("test", s3_client=StaticS3(CORRUPT)).resolve(alg_envelope(alg))


def test_validation_disabled_returns_stored_bytes():
    result = LfsResolver("test", s3_client=StaticS3(CORRUPT), validate_checksum=False).resolve(alg_envelope("md5"))
    assert result.payload == CORRUPT


def test_unsupported_checksum_algorithm_is_rejected():
    with pytest.raises(ValueError, match="unsupported checksum algorithm"):
        LfsResolver("test", s3_client=StaticS3(ORIGINAL)).resolve(envelope(checksum="x", checksum_alg="sha1"))
