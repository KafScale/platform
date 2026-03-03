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

package io.kafscale.examples.lfs;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.kafscale.lfs.AwsS3Reader;
import org.kafscale.lfs.LfsConsumer;
import org.kafscale.lfs.LfsEnvelope;
import org.kafscale.lfs.LfsProducer;
import org.kafscale.lfs.LfsResolver;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class E70JavaLfsDemo {
    public static void main(String[] args) throws Exception {
        
        String httpEndpoint = env("LFS_HTTP_ENDPOINT", "http://localhost:8080/lfs/produce");
        String topic = env("LFS_TOPIC", "lfs-demo-topic");
        String bootstrap = env("KAFKA_CONSUMER_BOOTSTRAP", env("KAFKA_BOOTSTRAP", "localhost:9092"));
        String bucket = env("S3_BUCKET", "kafscale");
        String s3Endpoint = env("S3_ENDPOINT", "http://localhost:9000");
        String s3Region = env("S3_REGION", "us-east-1");
        boolean pathStyle = Boolean.parseBoolean(env("S3_PATH_STYLE", "true"));
        String accessKey = env("AWS_ACCESS_KEY_ID", "minioadmin");
        String secretKey = env("AWS_SECRET_ACCESS_KEY", "minioadmin");
        String inlineThreshold = env("LFS_INLINE_THRESHOLD", "102400");

        int payloadSize = Integer.parseInt(env("LFS_PAYLOAD_SIZE", "524288"));
        String requestId = "e70-" + UUID.randomUUID();
        byte[] payload = new byte[payloadSize];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 251);
        }
        LfsProducer producer = new LfsProducer(URI.create(httpEndpoint));
        Map<String, String> headers = Map.of(
                "content-type", "application/octet-stream",
                "LFS_BLOB", "true",
                "LFS_INLINE_THRESHOLD", inlineThreshold,
                "X-Request-ID", requestId
        );
        LfsEnvelope envelope = producer.produce(
                topic,
                null,
                new ByteArrayInputStream(payload),
                headers,
                payload.length
        );
        System.out.println("Produced envelope: key=" + envelope.key + " sha256=" + envelope.sha256 + " requestId=" + requestId);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "e70-java-lfs-demo");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            S3Client s3 = buildS3Client(s3Endpoint, s3Region, pathStyle, accessKey, secretKey);
            LfsResolver resolver = new LfsResolver(new AwsS3Reader(s3, bucket), true, 0);
            LfsConsumer lfsConsumer = new LfsConsumer(consumer, resolver);

            Instant deadline = Instant.now().plusSeconds(30);
            boolean resolved = false;
            while (Instant.now().isBefore(deadline)) {
                var resolvedRecords = lfsConsumer.pollResolved(Duration.ofSeconds(2));
                if (resolvedRecords.isEmpty()) {
                    continue;
                }
                var record = resolvedRecords.get(0);
                System.out.println("Resolved record: isEnvelope=" + record.isEnvelope + " payloadBytes=" + record.payload.length);
                resolved = true;
                break;
            }
            if (!resolved) {
                System.out.println("No records resolved within timeout.");
            }
        }
    }

    private static S3Client buildS3Client(String endpoint, String region, boolean pathStyle, String accessKey, String secretKey) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(pathStyle).build())
                .build();
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private E70JavaLfsDemo() {
    }
}
