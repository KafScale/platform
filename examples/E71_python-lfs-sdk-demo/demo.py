# Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
# This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import hashlib
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

import boto3
from botocore.config import Config
from confluent_kafka import Consumer

from lfs_sdk import LfsProducer, LfsResolver


@dataclass
class VideoSpec:
    """Specification for a test video upload."""
    name: str
    size_bytes: int
    content_type: str = "video/mp4"


# Video size presets
VIDEO_SIZES = {
    "small": VideoSpec("small-clip.mp4", 1 * 1024 * 1024),      # 1 MB
    "midsize": VideoSpec("promo-video.mp4", 50 * 1024 * 1024),   # 50 MB
    "large": VideoSpec("full-feature.mp4", 200 * 1024 * 1024),   # 200 MB
}


def env(key: str, default: str) -> str:
    value = os.getenv(key)
    return value if value else default


def generate_video_data(size_bytes: int) -> bytes:
    """Generate synthetic video-like data with MP4-like header."""
    # Start with ftyp box header (MP4 signature)
    header = b"\x00\x00\x00\x1c" + b"ftypisom" + b"\x00\x00\x02\x00" + b"isomiso2mp41"

    # Fill rest with deterministic pattern for reproducibility
    remaining = size_bytes - len(header)
    if remaining <= 0:
        return header[:size_bytes]

    # Use a repeating pattern that compresses reasonably
    chunk = bytes(range(256)) * (remaining // 256 + 1)
    return header + chunk[:remaining]


def run_video_upload_test(
    producer: LfsProducer,
    resolver: LfsResolver,
    consumer: Consumer,
    video: VideoSpec,
    topic: str,
    timeout_seconds: int = 60,
) -> bool:
    """
    Run a single video upload test.

    Returns True if successful, False otherwise.
    """
    print(f"\n--- Testing {video.name} ({video.size_bytes / (1024*1024):.1f} MB) ---")

    # Generate video data
    print(f"  Generating {video.size_bytes} bytes...")
    start = time.time()
    data = generate_video_data(video.size_bytes)
    expected_sha256 = hashlib.sha256(data).hexdigest()
    gen_time = time.time() - start
    print(f"  Generated in {gen_time:.2f}s, sha256={expected_sha256[:16]}...")

    # Upload via LFS proxy
    print(f"  Uploading to LFS proxy...")
    start = time.time()
    try:
        envelope = producer.produce(
            topic,
            data,
            key=video.name.encode("utf-8"),
            headers={
                "Content-Type": video.content_type,
                "X-Video-Name": video.name,
            },
        )
        upload_time = time.time() - start
        print(f"  Upload completed in {upload_time:.2f}s")
        print(f"  Envelope: key={envelope.get('key')}, sha256={envelope.get('sha256')[:16]}...")
    except Exception as ex:
        print(f"  FAILED: Upload error: {ex}")
        return False

    # Verify the envelope sha256 matches
    if envelope.get("sha256") != expected_sha256:
        print(f"  FAILED: SHA256 mismatch! expected={expected_sha256}, got={envelope.get('sha256')}")
        return False

    # Consume and resolve the record
    print(f"  Consuming from topic {topic}...")
    deadline = time.time() + timeout_seconds
    resolved = False

    while time.time() < deadline:
        msg = consumer.poll(2.0)
        if msg is None:
            continue
        if msg.error():
            print(f"  Consumer error: {msg.error()}")
            continue

        # Check if this is our record by key
        msg_key = msg.key()
        if msg_key and msg_key.decode("utf-8", errors="ignore") == video.name:
            record = resolver.resolve(msg.value())
            print(f"  Resolved: is_envelope={record.is_envelope}, payload_bytes={len(record.payload)}")

            if record.is_envelope:
                # Verify resolved payload matches original
                resolved_sha256 = hashlib.sha256(record.payload).hexdigest()
                if resolved_sha256 != expected_sha256:
                    print(f"  FAILED: Resolved payload SHA256 mismatch!")
                    return False
                print(f"  SUCCESS: Payload verified ({len(record.payload)} bytes)")
                resolved = True
                break

    if not resolved:
        print(f"  FAILED: No matching record found within {timeout_seconds}s timeout")
        return False

    return True


def main() -> int:
    http_endpoint = env("LFS_HTTP_ENDPOINT", "http://localhost:8080/lfs/produce")
    topic = env("LFS_TOPIC", "video-raw")
    bootstrap = env("KAFKA_BOOTSTRAP", "localhost:9092")
    bucket = env("S3_BUCKET", "kafscale-lfs")
    s3_endpoint = env("S3_ENDPOINT", "http://localhost:9000")
    s3_region = env("S3_REGION", "us-east-1")
    access_key = env("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = env("AWS_SECRET_ACCESS_KEY", "minioadmin")

    # Parse which video sizes to test
    sizes_arg = env("VIDEO_SIZES", "small,midsize,large")
    requested_sizes = [s.strip() for s in sizes_arg.split(",")]
    videos_to_test: List[VideoSpec] = []

    for size in requested_sizes:
        if size in VIDEO_SIZES:
            videos_to_test.append(VIDEO_SIZES[size])
        else:
            print(f"Warning: Unknown video size '{size}', skipping")

    if not videos_to_test:
        print("No valid video sizes specified. Available: small, midsize, large")
        return 1

    print(f"=== Python LFS SDK Video Upload Demo ===")
    print(f"Endpoint: {http_endpoint}")
    print(f"Topic: {topic}")
    print(f"Bucket: {bucket}")
    print(f"Videos to test: {[v.name for v in videos_to_test]}")
    print()

    # Initialize producer
    producer = LfsProducer(http_endpoint)

    # Initialize Kafka consumer
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"e71-python-video-demo-{int(time.time())}",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([topic])

    # Initialize S3 client and resolver
    s3_client = boto3.client(
        "s3",
        endpoint_url=s3_endpoint,
        region_name=s3_region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "path"}),
    )
    resolver = LfsResolver(bucket=bucket, s3_client=s3_client)

    # Run tests
    results = []
    for video in videos_to_test:
        success = run_video_upload_test(producer, resolver, consumer, video, topic)
        results.append((video.name, success))

    # Cleanup
    consumer.close()
    producer.close()

    # Summary
    print("\n=== Test Summary ===")
    all_passed = True
    for name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"  {name}: {status}")
        if not success:
            all_passed = False

    if all_passed:
        print("\nAll video upload tests passed!")
        return 0
    else:
        print("\nSome tests failed.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
