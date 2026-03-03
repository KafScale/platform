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

# Industrial LFS Demo (E62)

This demo showcases LFS (Large File Support) for manufacturing/IoT workloads, demonstrating **mixed payload handling** where small telemetry passes through while large inspection images use LFS.

## Quick Start

```bash
make lfs-demo-industrial
```

## What This Demonstrates

- **Mixed payload handling**: Small telemetry (1KB) + large images (200MB) in same stream
- **Automatic routing**: LFS proxy routes based on `LFS_BLOB` header
- **Content explosion pattern**: Single factory stream spawns multiple derived topics
- **Real-time + batch**: Telemetry for real-time, images for batch analytics
- **OT/IT convergence**: Unified Kafka interface for both workloads

## Architecture

```
Factory Stream ──► LFS Proxy ──► Routing Decision
                                      │
        ┌─────────────────────────────┼─────────────────────────────┐
        ▼                             ▼                             ▼
  Small Telemetry              Large Images                   Derived Events
  (passthrough)                (LFS → S3)                     (processed)
        │                             │                             │
        ▼                             ▼                             ▼
sensor-telemetry           inspection-images              defect-events
   (no LFS)                  (LFS pointer)               (anomaly alerts)
```

## Content Explosion Topics

| Topic | Purpose | Contains LFS? | Typical Size |
|-------|---------|---------------|--------------|
| `sensor-telemetry` | Real-time sensor readings | No | 1KB |
| `inspection-images` | Thermal/visual inspection | Yes | 200MB |
| `defect-events` | Anomaly detection alerts | No | 2KB |
| `quality-reports` | Aggregated metrics | No | 10KB |

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `INDUSTRIAL_DEMO_NAMESPACE` | `kafscale-industrial` | Kubernetes namespace |
| `INDUSTRIAL_DEMO_TELEMETRY_COUNT` | `100` | Number of telemetry messages |
| `INDUSTRIAL_DEMO_IMAGE_COUNT` | `5` | Number of inspection images |
| `INDUSTRIAL_DEMO_IMAGE_SIZE` | `209715200` (200MB) | Inspection image size |
| `INDUSTRIAL_DEMO_CLEANUP` | `1` | Cleanup resources after demo |

## Sample Output

```
[1/8] Setting up industrial LFS demo environment...
[2/8] Deploying LFS proxy and MinIO...
[3/8] Creating content explosion topics...
      - sensor-telemetry (passthrough)
      - inspection-images (LFS)
      - defect-events (derived)
[4/8] Generating mixed workload...
      Telemetry: 100 readings (temp, pressure, vibration)
      Images: 5 thermal inspections (200MB each)
[5/8] Producing to LFS proxy...
      80 telemetry → passthrough (no LFS header)
      20 images → LFS (with LFS_BLOB header)
[6/8] Consuming records...
+-------------------+--------+----------+--------+
| Type              | Topic  | Count    | LFS?   |
+-------------------+--------+----------+--------+
| Telemetry         | sensor | 80       | No     |
| Inspection Image  | images | 5        | Yes    |
| Defect Alert      | defect | 2        | No     |
+-------------------+--------+----------+--------+
[7/8] Verifying blobs in MinIO...
      S3 blobs found: 5
[8/8] Mixed workload summary:
      Telemetry passthrough: 80 messages (80KB total)
      LFS uploads: 5 images (1GB total)
      Derived events: 7 alerts
```

## Real-World Use Cases

### Quality Inspection Station
- Cameras capture visual inspection images (large)
- PLC sends pass/fail signals (small)
- Both on same Kafka stream, LFS handles routing
- AI inference triggered on image topic

### Predictive Maintenance
- Vibration sensors stream continuously (small)
- Periodic thermal snapshots for analysis (large)
- Defect prediction writes to alert topic
- Dashboard consumes telemetry in real-time

### Assembly Line Monitoring
- Part tracking events (small)
- Robot vision captures for QA (large)
- Unified audit trail via Kafka
- S3 archive for compliance

## Why Manufacturing Buyers Care

1. **OT + IT Convergence**: Single Kafka interface for all factory data
2. **Real-time + Batch**: Telemetry for dashboards, images for ML training
3. **Cost Optimization**: Small data in Kafka, large data in S3
4. **Existing Infrastructure**: Works with standard Kafka clients

## Mixed Payload Example

```bash
# Small telemetry (passthrough - no LFS header)
echo '{"sensor":"temp-001","value":23.5}' | \
  kafka-console-producer --broker-list lfs-proxy:9092 --topic sensor-telemetry

# Large image (LFS - with LFS_BLOB header)
curl -X POST \
  -H "X-Kafka-Topic: inspection-images" \
  -H "X-Kafka-Key: station-A-001" \
  -H "Content-Type: image/thermal" \
  --data-binary @thermal-capture.raw \
  http://lfs-proxy:8080/lfs/produce
```

## Next Steps

- Connect to real OPC-UA/MQTT sources
- Add real thermal camera integration
- Integrate ML inference for defect detection
- Connect to Iceberg for historical analytics

## Files

| File | Description |
|------|-------------|
| `README.md` | This documentation |
| `../../scripts/industrial-lfs-demo.sh` | Main demo script |
