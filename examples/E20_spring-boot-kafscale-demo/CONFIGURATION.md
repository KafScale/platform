# Spring Boot Demo Configuration

This document describes how to configure the Spring Boot application to connect to the KafScale broker in different environments.

## Common Requirement: DNS

The broker is configured to advertise itself as **`kafscale-broker:39092`**.
Therefore, in **ALL** environments, the client must be able to resolve `kafscale-broker` to an IP address that accepts traffic on port **39092**.

---

## (a) Local Run (via `mvn`)

**Use case**: Running `mvn spring-boot:run` or `mvn exec:java` directly on your machine.

1.  **Network Setup**:
    *   Update your `/etc/hosts` file (or Windows `hosts` file) to map the hostname to localhost.
        ```bash
        echo "127.0.0.1 kafscale-broker" | sudo tee -a /etc/hosts
        ```
    *   Ensure the port forwarding is active (provided by `make demo-guide-pf`).
        ```bash
        kubectl -n kafscale-demo port-forward svc/kafscale-broker 39092:39092
        ```

2.  **Configuration**:
    *   File: `src/main/resources/application.yml`
    *   Value:
        ```yaml
        spring:
          kafka:
            bootstrap-servers: kafscale-broker:39092
        ```

---

## (b) Docker Container

**Use case**: Running the app as a Docker container on your local machine.

### Option 1: Host Networking (Linux/Simple)
Running with `--network host` allows the container to share the host's networking stack.
*   **Command**: `docker run --network host ...`
*   **Config**: Same as "Local Run" (hosts file on host machine handles resolution, port forward on host handles traffic).

### Option 2: Bridge Networking (Mac/Windows/Default)
The container needs to reach the host machine where the port-forward is running.

1.  **Network Setup**:
    *   Services exposed on the host via `kubectl port-forward` are accessible via `host.docker.internal` (Docker Desktop).
    *   You need to map `kafscale-broker` to the IP of `host.docker.internal` inside the container.
    *   **Command**:
        ```bash
        docker run \
          --add-host kafscale-broker:host-gateway \
          ... \
          ghcr.io/novatechflow/kafscale-spring-demo:dev
        ```
        *(Note: `host-gateway` resolves to the host IP).*

2.  **Configuration**:
    *   Env Var: `SPRING_KAFKA_BOOTSTRAP_SERVERS=kafscale-broker:39092`

---

## (c) Kubernetes Pod

**Use case**: Running inside the Kubernetes cluster (e.g., `make demo-guide-pf`).

1.  **Network Setup**:
    *   The internal Kubernetes Service `kafscale-broker` must expose port **39092**.
    *   (Note: Since the default broker port is 9092, we patch the service to also listen on 39092 and forward to 9092).

2.  **Configuration**:
    *   The App Pod resolves `kafscale-broker` to the ClusterIP of the Service via KubeDNS.
    *   Env Var Override in Deployment (`deploy/demo/spring-boot-app.yaml`):
        ```yaml
        env:
          - name: SPRING_KAFKA_BOOTSTRAP_SERVERS
            value: kafscale-broker:39092
        ```
