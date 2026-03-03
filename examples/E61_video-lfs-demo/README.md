<!--
Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
This project is supported and financed by Scalytics, Inc. (www.scalytics.io).

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Video LFS Demo (E61)

This demo showcases LFS (Large File Support) for video/media workflows, demonstrating the **content explosion pattern** with large video files.

## Quick Start

```bash
make lfs-demo-video
```

## What This Demonstrates

- **Large blob handling**: 2GB+ video files stored in S3 via LFS
- **Content explosion pattern**: Single upload spawns multiple derived topics
- **Metadata extraction**: Duration, codec, resolution extracted to separate topic
- **Frame references**: Keyframe extraction pointers for thumbnails
- **Streaming upload**: HTTP streaming for memory-efficient uploads

## Architecture

```
Video Upload ──► LFS Proxy ──► S3 (blob) + Kafka (pointer)
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              video-raw       video-meta      video-frames
            (LFS pointer)   (codec, duration) (keyframe refs)
```

## Content Explosion Topics

| Topic | Purpose | Contains LFS? |
|-------|---------|---------------|
| `video-raw` | Original video blob pointer | Yes |
| `video-metadata` | Duration, codec, resolution, bitrate | No |
| `video-frames` | Keyframe timestamps and S3 refs | No |
| `video-ai-tags` | Scene detection, object labels | No |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `VIDEO_DEMO_NAMESPACE` | `kafscale-video` | Kubernetes namespace |
| `VIDEO_DEMO_BLOB_SIZE` | `2147483648` (2GB) | Video file size |
| `VIDEO_DEMO_BLOB_COUNT` | `2` | Number of videos to upload |
| `VIDEO_DEMO_CLEANUP` | `1` | Cleanup resources after demo |

## Sample Output

```
[1/8] Setting up video LFS demo environment...
[2/8] Deploying LFS proxy and MinIO...
[3/8] Creating content explosion topics...
      - video-raw (LFS blobs)
      - video-metadata (codec, duration)
      - video-frames (keyframe refs)
[4/8] Generating synthetic video data...
      Video: promo-2026-01.mp4, Codec: H.264, Size: 2GB
      Video: webinar-2026-02.mp4, Codec: H.265, Size: 2GB
[5/8] Uploading via LFS proxy...
[6/8] Consuming pointer records...
+----------------------+------------------------------------------------------------------+--------+
| Video                | SHA256                                                           | Status |
+----------------------+------------------------------------------------------------------+--------+
| promo-2026-01.mp4    | d4e5f6a1b2c3...                                                  | ok     |
| webinar-2026-02.mp4  | e5f6a1b2c3d4...                                                  | ok     |
+----------------------+------------------------------------------------------------------+--------+
[7/8] Verifying blobs in MinIO...
      S3 blobs found: 2
[8/8] Content explosion summary:
      video-raw: 2 LFS pointers
      video-metadata: 2 codec records
      video-frames: 120 keyframe refs (simulated)
```

## Real-World Use Cases

### Video Platform
- User uploads 4K video content
- Automatic transcoding pipeline triggered via derived topic
- Thumbnail generation from keyframe references
- CDN distribution metadata published

### Security Camera System
- Continuous footage stored efficiently
- Motion detection events as derived messages
- Forensic search via metadata topics
- Retention policy enforcement

### Live Streaming Archive
- VOD assets stored post-broadcast
- Clip extraction references
- Analytics events (views, engagement)
- AI-powered content moderation

## Why Media Buyers Care

1. **Scale**: Video files are inherently large - LFS handles multi-GB files
2. **Pipeline**: Content explosion enables parallel processing workflows
3. **Cost**: S3 storage is cheaper than Kafka retention for large blobs
4. **Flexibility**: Decoupled metadata enables independent scaling

## Streaming Upload Example

```bash
# Upload a video file via HTTP streaming
curl -X POST \
  -H "X-Kafka-Topic: video-raw" \
  -H "X-Kafka-Key: my-video-001" \
  -H "Content-Type: video/mp4" \
  --data-binary @my-video.mp4 \
  http://lfs-proxy:8080/lfs/produce
```

## Next Steps

- Connect to real video source (ffmpeg, OBS)
- Add real metadata extraction with ffprobe
- Integrate transcoding pipeline (FFmpeg workers)
- Add AI tagging with scene detection model

## Files

| File | Description |
|------|-------------|
| `README.md` | This documentation |
| `../../scripts/video-lfs-demo.sh` | Main demo script |
