# Java Kafka Client Demo (E10)

This example demonstrates a basic Java application using the standard `kafka-clients` library to interact with a KafScale cluster.

## Features

The `SimpleDemo` application performs the following actions:
1.  **Admin Operations**: Checks if a topic exists, creating it if necessary (`demo-topic-1`).
2.  **Producer**: Sends 25 messages to the topic.
3.  **Admin Operations**: Retrieves and displays cluster metadata (Cluster ID, Controller, Nodes).
4.  **Consumer**: Subscribes to the topic and consumes messages (waits for up to 5 messages or timeout).

## Prerequisites

-   Java 17+
-   Maven 3.6+
-   A running KafScale cluster (e.g., local Docker Compose setup)

## Configuration

The connection details are currently hardcoded in `src/main/java/com/example/kafscale/SimpleDemo.java`:

```java
private static final String BOOTSTRAP_SERVERS = "127.0.0.1:39092";
```

This configuration assumes you are running the demo locally and connecting to the KafScale broker exposed on port `39092` (standard local setup).

## Running the Demo

1.  Make sure your KafScale cluster is running (e.g., via `docker-compose up` in the root directory).
2.  Run the application using Maven:

```bash
mvn clean package exec:java
```

## Expected Output

You should see logs indicating the progression of the demo:

```text
[INFO] Starting SimpleDemo...
[INFO] Topic demo-topic-1 already exists.
[INFO] Sent message: key=key-0 value=message-0 partition=0 offset=0
...
[INFO] Cluster Metadata:
[INFO]   Cluster ID: ...
[INFO]   Nodes (Advertised Listeners): ...
[INFO] Received message: key=key-0 value=message-0 partition=0 offset=0
...
[INFO] Successfully consumed 5 messages.
```
