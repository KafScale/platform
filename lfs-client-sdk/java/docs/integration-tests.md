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

# Java SDK Integration Tests (TestContainers)

## Overview
The Java SDK integration tests spin up Kafka, MinIO, and the LFS proxy with TestContainers to validate end-to-end LFS produce and resolve behavior.

## What Is Covered
- Produce via HTTP `/lfs/produce` and receive an envelope.
- Consume the pointer record from Kafka.
- Fetch the blob from MinIO and validate content.
- Backend down returns 5xx with structured error codes.

## Containers
- Kafka: `confluentinc/cp-kafka:7.6.1` (default)
- MinIO: `quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z` (default)
- LFS proxy: `ghcr.io/kafscale/kafscale-lfs-proxy:dev` (default)

Override images with environment variables:

- `KAFSCALE_KAFKA_IMAGE`
- `KAFSCALE_MINIO_IMAGE`
- `KAFSCALE_LFS_PROXY_IMAGE`

## Running
```bash
mvn test
```

## SDK Makefile (Multi-language)
You can run Java build/tests via the SDK-level Makefile:

```bash
make -C lfs-client-sdk build-java
make -C lfs-client-sdk test-java
```

## Notes
- Tests require Docker to be running.
- The proxy is configured with path-style S3 for MinIO compatibility.
- The Kafka backend is stopped in one test to validate 5xx error handling.

## Docker Requirement
Integration tests are automatically skipped when Docker is not available.

- `@Testcontainers(disabledWithoutDocker = true)`
- Ensure Docker is running to execute them.

## Debugging Docker Detection (Mac Docker Desktop)
Purpose: document the steps and insights used to debug TestContainers when tests are skipped even though Docker Desktop is running.

### Symptoms
- Tests are skipped with `disabledWithoutDocker = true`.
- TestContainers reports "Could not find a valid Docker environment".
- Diagnostic logs show Docker Desktop socket but API calls return HTTP 400.

### Root Cause Insights
- TestContainers does **not** read `lfs-client-sdk/java/src/test/resources/.testcontainers.properties`.
- It reads only `~/.testcontainers.properties` or environment variables.
- Docker Desktop exposes two sockets on macOS:
  - `~/Library/Containers/com.docker.docker/Data/docker-cli.sock` (CLI metadata socket, returns HTTP 400 for `GET /info`)
  - `~/.docker/run/docker.sock` (actual Docker API socket)

TestContainers was defaulting to the Docker Desktop CLI socket, which rejects API calls with HTTP 400. The fix is to point TestContainers to the real Docker API socket.

### Steps That Worked
1) Verify Docker is reachable (from host):
```bash
docker info
```

2) Create or update user config so TestContainers uses the real socket:
```bash
cat > ~/.testcontainers.properties <<'EOF'
docker.host=unix:///Users/kamir/.docker/run/docker.sock
EOF
```

3) Re-run the integration tests:
```bash
mvn -Dtest=LfsProducerIntegrationTest test
```

### Optional Diagnostics
Run the lightweight diagnostic test to print detection details:
```bash
mvn -Dtest=DockerAvailabilityTest test
```

If you need extra logs, ensure a test-only SLF4J binding is present (already added in `pom.xml`):
- `org.slf4j:slf4j-simple` (test scope)







You already have a solid and mostly complete runbook. I will focus on what actions you should take next and how to improve this document so that another developer can reliably execute the integration tests without guessing.

⸻

What You Should Do

1) First: Validate the Environment Assumptions

Before touching any code, ensure these preconditions are true on the machine where tests run:

Run these checks

# A – Docker must be reachable from the shell
docker info

# B – Confirm the real API socket exists (macOS)
ls -l ~/.docker/run/docker.sock

If either fails, the integration tests will correctly be skipped.

⸻

2) Standardize TestContainers Configuration

Your main finding is important:

TestContainers ignores
lfs-client-sdk/java/src/test/resources/.testcontainers.properties

Action

Make this explicit and enforce one of two supported mechanisms:

Option A – User level (recommended for local dev)
Create:

~/.testcontainers.properties

with:

docker.host=unix:///Users/<username>/.docker/run/docker.sock

Option B – CI / project portable approach
Use environment variable instead:

export DOCKER_HOST=unix:///Users/<username>/.docker/run/docker.sock

You should add to the README
	•	A short explanation that only two locations are honored:
	•	~/.testcontainers.properties
	•	environment variables

Not the test resources folder.

⸻

3) Add a Mandatory Preflight Test

You already created DockerAvailabilityTest.
This should become the first test in the suite.

Action
	•	Keep this class.
	•	Document it as the canonical diagnostic.

mvn -Dtest=DockerAvailabilityTest test

If this fails → all other integration tests are expected to be skipped.

⸻

4) Improve the Document Structure

I recommend turning your text into:
	1.	Quick Start
	2.	Troubleshooting Decision Tree
	3.	macOS Specific Fix
	4.	CI Notes

Concrete Edits You Should Apply

A – Add a “First 60 Seconds Check”
At top of Running section:

### 0) Preflight Check

mvn -Dtest=DockerAvailabilityTest test

Expected:
	•	Docker environment detected
	•	API version printed
	•	No HTTP 400

If this fails → follow macOS socket fix.

#### B – Make Root Cause More Explicit

Replace:

> Docker Desktop exposes two sockets on macOS

with a clearer table:

| Socket | Purpose | Works with TestContainers |
|------|---------|-----------------------------|
| `~/Library/.../docker-cli.sock` | CLI metadata | NO – returns HTTP 400 |
| `~/.docker/run/docker.sock` | Real Docker API | YES |

---

## 5) Verify the Test Design Itself

Your integration flow is good:

1. HTTP produce  
2. Kafka pointer record  
3. MinIO blob  
4. Backend down → 5xx  

### You should additionally check

- Are containers started with `withReuse(false)`?  
  → Good for isolation.

- Do you wait for:
  - Kafka ready  
  - MinIO ready  
  - Proxy health endpoint  

If not, add explicit wait strategies.

---

## 6) Next Concrete Steps for You

### Step 1 – On your machine

Run exactly:

```bash
docker info
mvn -Dtest=DockerAvailabilityTest test
mvn -Dtest=LfsProducerIntegrationTest test

Step 2 – Update the repository README

Add:
	•	the socket explanation table
	•	exact location rules
	•	preflight test command
	•	CI section

Step 3 – Optional Hardening

Consider adding:
	•	environment variable fallback in tests:

.withEnv("DOCKER_HOST",
    System.getenv().getOrDefault(
        "DOCKER_HOST",
        "unix:///Users/.../.docker/run/docker.sock"))


⸻

Final Recommendation

You do not need code changes in the proxy or tests.
The issue is 100% TestContainers configuration discovery on macOS.

Your document is correct; it just needs:
	•	stronger ordering
	•	explicit decision tree
	•	clearer socket distinction
	•	mandatory preflight step

⸻

If you want, I can help you convert this into:
	•	a ready-to-merge README patch
	•	a CONTRIBUTING.md section
	•	or a CI GitHub Action snippet that auto-detects the correct socket.




  Below is a drop-in GitHub Actions snippet that auto-detects the correct Docker socket on macOS runners and falls back to the default on Linux.
It exports DOCKER_HOST only when needed so TestContainers works without manual .testcontainers.properties.

⸻

GitHub Action Job Snippet

jobs:
  integration-tests:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest]

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK
        uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: 17
          cache: maven

      - name: Detect Docker socket (macOS fix for TestContainers)
        shell: bash
        run: |
          echo "Operating system: $RUNNER_OS"

          if [[ "$RUNNER_OS" == "macOS" ]]; then
            # Preferred real Docker API socket
            REAL_SOCKET="$HOME/.docker/run/docker.sock"

            if [[ -S "$REAL_SOCKET" ]]; then
              echo "Using real Docker API socket: $REAL_SOCKET"
              echo "DOCKER_HOST=unix://$REAL_SOCKET" >> $GITHUB_ENV
            else
              echo "Real Docker socket not found, falling back to default"
            fi
          fi

          echo "Docker info for diagnostics:"
          docker info || true

      - name: Run Docker availability preflight
        run: |
          mvn -B -Dtest=DockerAvailabilityTest test

      - name: Run integration tests
        run: |
          mvn -B test


⸻

What This Solves

1) macOS Problem
	•	Docker Desktop exposes two sockets
	•	TestContainers often picks the CLI metadata socket → HTTP 400
	•	This forces the real API socket:

~/.docker/run/docker.sock

2) Linux
	•	No change, default Docker env is used.

3) Safe Behavior
	•	If socket not found → does nothing
	•	Tests will be skipped via:

@Testcontainers(disabledWithoutDocker = true)


⸻

Optional: Add Explicit Env for Image Overrides

If you want to parameterize container images in CI:

      - name: Run integration tests
        env:
          KAFSCALE_KAFKA_IMAGE: confluentinc/cp-kafka:7.6.1
          KAFSCALE_MINIO_IMAGE: quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z
          KAFSCALE_LFS_PROXY_IMAGE: ghcr.io/kafscale/kafscale-lfs-proxy:dev
        run: mvn -B test


⸻

Optional Robust Variant (with detection log)

If you want extra visibility:

      - name: Debug socket candidates
        if: runner.os == 'macOS'
        run: |
          echo "Checking known sockets:"
          ls -la ~/Library/Containers/com.docker.docker/Data/ || true
          ls -la ~/.docker/run/ || true


⸻

Result
	•	Works on:
	•	macOS GitHub runners
	•	local macOS
	•	Linux CI
	•	No .testcontainers.properties needed
	•	No manual user setup
	•	Fully portable

⸻

If you want, I can add:
	•	retry logic when Docker Desktop is still starting
	•	a wait loop for docker info
	•	caching for TestContainers Ryuk image to speed up CI.
