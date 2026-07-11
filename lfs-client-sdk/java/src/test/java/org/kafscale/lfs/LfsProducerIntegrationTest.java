// Copyright 2026 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
// This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.kafscale.lfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers(disabledWithoutDocker = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class LfsProducerIntegrationTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String BUCKET = "kafscale";
    private static final String TOPIC = "lfs-demo-topic";
    private static final String MINIO_USER = "minioadmin";
    private static final String MINIO_PASS = "minioadmin";

    private static final DockerImageName KAFKA_IMAGE = DockerImageName.parse(
            System.getenv().getOrDefault("KAFSCALE_KAFKA_IMAGE", "confluentinc/cp-kafka:7.6.1"));
    private static final DockerImageName MINIO_IMAGE = DockerImageName.parse(
            System.getenv().getOrDefault("KAFSCALE_MINIO_IMAGE", "quay.io/minio/minio:RELEASE.2024-09-22T00-33-43Z"));
    private static final DockerImageName LFS_PROXY_IMAGE = DockerImageName.parse(
            System.getenv().getOrDefault("KAFSCALE_LFS_PROXY_IMAGE", "ghcr.io/kafscale/kafscale-lfs-proxy:dev"));

    private static final Network NETWORK = Network.newNetwork();

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("kafka");

    @Container
    private static final GenericContainer<?> MINIO = new GenericContainer<>(MINIO_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("minio")
            .withEnv("MINIO_ROOT_USER", MINIO_USER)
            .withEnv("MINIO_ROOT_PASSWORD", MINIO_PASS)
            .withCommand("server", "/data", "--console-address", ":9001")
            .withExposedPorts(9000);

    @Container
    private static final GenericContainer<?> LFS_PROXY = new GenericContainer<>(LFS_PROXY_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases("lfs-proxy")
            .withEnv("KAFSCALE_LFS_PROXY_ADDR", ":9092")
            .withEnv("KAFSCALE_LFS_PROXY_ADVERTISED_HOST", "lfs-proxy")
            .withEnv("KAFSCALE_LFS_PROXY_ADVERTISED_PORT", "9092")
            .withEnv("KAFSCALE_LFS_PROXY_HTTP_ADDR", ":8080")
            .withEnv("KAFSCALE_LFS_PROXY_HEALTH_ADDR", ":9094")
            .withEnv("KAFSCALE_LFS_PROXY_BACKENDS", "kafka:9092")
            .withEnv("KAFSCALE_LFS_PROXY_S3_BUCKET", BUCKET)
            .withEnv("KAFSCALE_LFS_PROXY_S3_REGION", "us-east-1")
            .withEnv("KAFSCALE_LFS_PROXY_S3_ENDPOINT", "http://minio:9000")
            .withEnv("KAFSCALE_LFS_PROXY_S3_FORCE_PATH_STYLE", "true")
            .withEnv("KAFSCALE_LFS_PROXY_S3_ENSURE_BUCKET", "true")
            .withEnv("KAFSCALE_LFS_PROXY_S3_ACCESS_KEY", MINIO_USER)
            .withEnv("KAFSCALE_LFS_PROXY_S3_SECRET_KEY", MINIO_PASS)
            .withExposedPorts(8080, 9094);

    static {
        if (isDiagnosticsEnabled()) {
            printDiagnostics();
        }
    }

    @Test
    @Order(1)
    void producesEnvelopeAndResolvesPayload() throws Exception {
        waitForReady();
        ensureBucket();

        byte[] payload = new byte[256 * 1024];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }

        LfsProducer producer = new LfsProducer(httpEndpoint());
        LfsEnvelope env = producer.produce(TOPIC, null, new java.io.ByteArrayInputStream(payload),
                Collections.singletonMap("content-type", "application/octet-stream"));

        assertNotNull(env);
        assertNotNull(env.key);
        assertEquals(BUCKET, env.bucket);

        LfsEnvelope consumed = consumeEnvelope();
        assertEquals(env.key, consumed.key);

        byte[] stored = fetchObject(consumed.key);
        assertTrue(stored.length > 0);
        assertEquals(payload.length, stored.length);
        assertTrue(Arrays.equals(payload, stored));
    }

    @Test
    @Order(2)
    void returns5xxWhenBackendUnavailable() throws Exception {
        waitForReady();
        KAFKA.stop();

        LfsProducer producer = new LfsProducer(httpEndpoint(), Duration.ofSeconds(2), Duration.ofSeconds(5));
        LfsHttpException ex = assertThrows(LfsHttpException.class,
                () -> producer.produce(TOPIC, null, new java.io.ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8)),
                        Collections.singletonMap("content-type", "application/octet-stream")));

        assertTrue(ex.getStatusCode() == 502 || ex.getStatusCode() == 503);
        assertTrue(ex.getErrorCode().equals("backend_unavailable") || ex.getErrorCode().equals("backend_error"));
    }

    private static URI httpEndpoint() {
        return URI.create("http://" + LFS_PROXY.getHost() + ":" + LFS_PROXY.getMappedPort(8080) + "/lfs/produce");
    }

    private static void waitForReady() throws InterruptedException {
        URI ready = URI.create("http://" + LFS_PROXY.getHost() + ":" + LFS_PROXY.getMappedPort(9094) + "/readyz");
        for (int i = 0; i < 30; i++) {
            try {
                java.net.http.HttpResponse<String> resp = java.net.http.HttpClient.newHttpClient().send(
                        java.net.http.HttpRequest.newBuilder().uri(ready).timeout(Duration.ofSeconds(2)).GET().build(),
                        java.net.http.HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() == 200) {
                    return;
                }
            } catch (Exception ignored) {
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("lfs-proxy not ready");
    }

    private static void ensureBucket() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        } catch (Exception ignored) {
        }
    }

    private static LfsEnvelope consumeEnvelope() throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "lfs-sdk-it");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            long deadline = System.currentTimeMillis() + 10000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    byte[] payload = records.iterator().next().value();
                    return MAPPER.readValue(payload, LfsEnvelope.class);
                }
            }
        }
        throw new IllegalStateException("no records consumed");
    }

    private static byte[] fetchObject(String key) throws Exception {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create("http://" + MINIO.getHost() + ":" + MINIO.getMappedPort(9000)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(MINIO_USER, MINIO_PASS)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            ResponseInputStream<?> stream = s3.getObject(GetObjectRequest.builder().bucket(BUCKET).key(key).build());
            return stream.readAllBytes();
        }
    }

    private static boolean isDiagnosticsEnabled() {
        String value = System.getenv("KAFSCALE_TC_DIAG");
        return value != null && (value.equalsIgnoreCase("1") || value.equalsIgnoreCase("true"));
    }

    private static void printDiagnostics() {
        System.err.println("[tc-diag] DOCKER_HOST=" + System.getenv("DOCKER_HOST"));
        System.err.println("[tc-diag] DOCKER_CONTEXT=" + System.getenv("DOCKER_CONTEXT"));
        System.err.println("[tc-diag] TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=" + System.getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"));
        System.err.println("[tc-diag] testcontainers.docker.socket.override=" + System.getProperty("testcontainers.docker.socket.override"));
        try {
            DockerClientFactory factory = DockerClientFactory.instance();
            System.err.println("[tc-diag] dockerAvailable=" + factory.isDockerAvailable());
            System.err.println("[tc-diag] dockerHostIp=" + factory.dockerHostIpAddress());
        } catch (Exception e) {
            System.err.println("[tc-diag] docker check failed: " + e.getClass().getName() + ": " + e.getMessage());
        }
    }
}
