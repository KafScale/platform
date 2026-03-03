from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Dict, Optional, Union

import requests


MULTIPART_MIN_BYTES = 5 * 1024 * 1024  # 5MB
DEFAULT_CONNECT_TIMEOUT = 10.0
DEFAULT_REQUEST_TIMEOUT = 300.0
DEFAULT_RETRIES = 3
RETRY_BASE_SLEEP_SECONDS = 0.2


@dataclass
class LfsHttpException(Exception):
    """Exception raised for LFS HTTP errors with structured error info."""
    status_code: int
    code: str
    message: str
    request_id: str
    body: str

    def __str__(self) -> str:
        return f"LfsHttpException({self.status_code}, code={self.code}, message={self.message}, request_id={self.request_id})"


class LfsProducer:
    """LFS producer with retry/backoff support for producing large blobs."""

    def __init__(
        self,
        endpoint: str,
        connect_timeout: float = DEFAULT_CONNECT_TIMEOUT,
        request_timeout: float = DEFAULT_REQUEST_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
    ) -> None:
        self.endpoint = endpoint
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.retries = retries
        self._session = requests.Session()

    def produce(
        self,
        topic: str,
        payload: Union[bytes, BinaryIO, Path],
        key: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> dict:
        """
        Produce a blob to the LFS proxy.

        Args:
            topic: Kafka topic name
            payload: Data to send - bytes, file-like object, or Path to a file
            key: Optional Kafka key
            headers: Optional additional headers

        Returns:
            LFS envelope dict from the proxy response
        """
        # Convert payload to bytes for proper Content-Length and retry support
        if isinstance(payload, Path):
            data = payload.read_bytes()
        elif hasattr(payload, "read"):
            data = payload.read()
        else:
            data = payload

        out_headers = {"X-Kafka-Topic": topic}
        if key is not None:
            out_headers["X-Kafka-Key"] = key.decode("utf-8", errors="ignore")
        if headers:
            out_headers.update(headers)
        if "X-Request-ID" not in out_headers:
            out_headers["X-Request-ID"] = str(uuid.uuid4())

        actual_size = len(data)
        out_headers["X-LFS-Size"] = str(actual_size)
        out_headers["X-LFS-Mode"] = "single" if actual_size < MULTIPART_MIN_BYTES else "multipart"

        return self._send_with_retry(data, out_headers)

    def _send_with_retry(self, data: bytes, headers: Dict[str, str]) -> dict:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self._session.post(
                    self.endpoint,
                    data=data,
                    headers=headers,
                    timeout=(self.connect_timeout, self.request_timeout),
                )
                if 200 <= resp.status_code < 300:
                    return resp.json()

                # Parse error response
                body = resp.text
                request_id = resp.headers.get("X-Request-ID", "")
                code = ""
                message = body
                try:
                    err = resp.json()
                    code = err.get("code", "")
                    message = err.get("message", body)
                    request_id = err.get("request_id", request_id)
                except Exception:
                    pass

                http_error = LfsHttpException(
                    status_code=resp.status_code,
                    code=code,
                    message=message,
                    request_id=request_id,
                    body=body,
                )

                # Retry on 5xx errors
                if resp.status_code >= 500 and attempt < self.retries:
                    last_error = http_error
                    self._sleep_backoff(attempt)
                    continue
                raise http_error

            except requests.exceptions.RequestException as ex:
                last_error = ex
                if attempt == self.retries:
                    break
                self._sleep_backoff(attempt)

        if last_error:
            raise last_error
        raise RuntimeError("produce failed: no response")

    def _sleep_backoff(self, attempt: int) -> None:
        sleep_time = RETRY_BASE_SLEEP_SECONDS * (2 ** (attempt - 1))
        time.sleep(sleep_time)

    def close(self) -> None:
        """Close the underlying session."""
        self._session.close()

    def __enter__(self) -> "LfsProducer":
        return self

    def __exit__(self, *args) -> None:
        self.close()


def produce_lfs(
    endpoint: str,
    topic: str,
    payload: Union[bytes, BinaryIO, Path],
    key: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = DEFAULT_REQUEST_TIMEOUT,
) -> dict:
    """
    Convenience function for one-shot LFS produce.

    For multiple produces, use LfsProducer for connection reuse.
    """
    with LfsProducer(endpoint, request_timeout=timeout) as producer:
        return producer.produce(topic, payload, key, headers)
