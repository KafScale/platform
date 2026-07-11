/*
 * Copyright 2025 Alexander Alten (novatechflow), NovaTechflow (novatechflow.com).
 * This project is supported and financed by Scalytics, Inc. (www.scalytics.io).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.kafscale;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDemo {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDemo.class);
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
    private static volatile KafkaConsumer<String, String> activeConsumer;

    public static void main(String[] args) {
        Config config = Config.load(args);

        logger.info("Starting SimpleDemo...");
        logger.info("Config: bootstrapServers={}, topic={}, produceCount={}, consumeCount={}, acks={}, idempotence={}, retries={}",
                config.bootstrapServers, config.topic, config.produceCount, config.consumeCount,
                config.acks, config.enableIdempotence, config.retries);

        // 0. Show all topics
        try {
            showTopics(config);
        } catch (Exception e) {
            logger.warn("Skipping showTopics due to error: {}", e.getMessage());
        }

        // 1. Ensure topic exists
        try {
            createTopic(config);
        } catch (Exception e) {
            logger.warn("Skipping createTopic due to error: {}", e.getMessage());
        }

        // 2. Produce 5 messages
        produceMessages(config);

        // 3. Show Cluster Metadata
        try {
            showClusterMetadata(config);
        } catch (Exception e) {
            logger.warn("Skipping showClusterMetadata due to error: {}", e.getMessage());
        }

        // 4. Consume messages
        consumeMessages(config);

    }

    private static void createTopic(Config config) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", String.valueOf(config.timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(config.timeoutMs * 2L));

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Check if topic exists
            boolean topicExists = adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(config.topic);

            if (!topicExists) {
                logger.info("Topic {} does not exist. Creating it...", config.topic);
                NewTopic newTopic = new NewTopic(config.topic, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
                logger.info("Topic {} created successfully.", config.topic);
            } else {
                logger.info("Topic {} already exists.", config.topic);
            }
        }
    }

    private static void produceMessages(Config config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(config.enableIdempotence));
        props.put(ProducerConfig.ACKS_CONFIG, config.acks);
        props.put(ProducerConfig.RETRIES_CONFIG, String.valueOf(config.retries));
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", String.valueOf(config.timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(config.timeoutMs * 2L));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < config.produceCount; i++) {
                String key = "key-" + i;
                String value = "message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(config.topic, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent message: key={} value={} partition={} offset={}",
                                key, value, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Error sending message", exception);
                    }
                }).get(5, TimeUnit.SECONDS); // Block for demonstration purposes
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error("Producer error", e);
            Thread.currentThread().interrupt();
        }
    }

    private static void consumeMessages(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", String.valueOf(config.timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(config.timeoutMs * 2L));

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            activeConsumer = consumer;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                RUNNING.set(false);
                KafkaConsumer<String, String> current = activeConsumer;
                if (current != null) {
                    current.wakeup();
                }
            }));
            consumer.subscribe(Collections.singletonList(config.topic));

            int messagesReceived = 0;
            long endTime = System.currentTimeMillis() + config.consumeTimeoutMs;

            try {
                while (RUNNING.get() && messagesReceived < config.consumeCount && System.currentTimeMillis() < endTime) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Received message: key={} value={} partition={} offset={}",
                                record.key(), record.value(), record.partition(), record.offset());
                        messagesReceived++;
                        if (messagesReceived >= config.consumeCount) {
                            break;
                        }
                    }
                }
            } catch (WakeupException e) {
                if (RUNNING.get()) {
                    throw e;
                }
            }

            if (messagesReceived < config.consumeCount) {
                logger.warn("Timed out waiting for messages. Received: {}", messagesReceived);
            } else {
                logger.info("Successfully consumed {} messages.", messagesReceived);
            }

        } catch (Exception e) {
            logger.error("Consumer error", e);
        } finally {
            activeConsumer = null;
        }
    }

    private static void showTopics(Config config) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", String.valueOf(config.timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(config.timeoutMs * 2L));

        try (AdminClient adminClient = AdminClient.create(props)) {
            logger.info("Listing topics...");
            adminClient.listTopics().names().get(5, TimeUnit.SECONDS).forEach(name -> logger.info("Topic: {}", name));
        }
    }

    private static void showClusterMetadata(Config config) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", String.valueOf(config.timeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(config.timeoutMs * 2L));

        try (AdminClient adminClient = AdminClient.create(props)) {
            org.apache.kafka.clients.admin.DescribeClusterResult result = adminClient.describeCluster();
            logger.info("Cluster Metadata:");
            logger.info("  Cluster ID: {}", result.clusterId().get());
            logger.info("  Controller: {}", result.controller().get());
            logger.info("  Nodes (Advertised Listeners):");
            result.nodes().get().forEach(node -> logger.info("    Node ID: {}, Host: {}, Port: {}, Rack: {}",
                    node.id(), node.host(), node.port(), node.hasRack() ? node.rack() : "null"));
        }
    }

    private static final class Config {
        final String bootstrapServers;
        final String topic;
        final int produceCount;
        final int consumeCount;
        final long consumeTimeoutMs;
        final String acks;
        final boolean enableIdempotence;
        final int retries;
        final int timeoutMs;
        final String groupId;

        private Config(String bootstrapServers, String topic, int produceCount, int consumeCount,
                long consumeTimeoutMs, String acks, boolean enableIdempotence, int retries, int timeoutMs, String groupId) {
            this.bootstrapServers = bootstrapServers;
            this.topic = topic;
            this.produceCount = produceCount;
            this.consumeCount = consumeCount;
            this.consumeTimeoutMs = consumeTimeoutMs;
            this.acks = acks;
            this.enableIdempotence = enableIdempotence;
            this.retries = retries;
            this.timeoutMs = timeoutMs;
            this.groupId = groupId;
        }

        static Config load(String[] args) {
            String bootstrap = envOrArg(args, "--bootstrap=", "KAFSCALE_BOOTSTRAP_SERVERS", "127.0.0.1:39092");
            String topic = envOrArg(args, "--topic=", "KAFSCALE_TOPIC", "demo-topic-1");
            int produceCount = parseInt(envOrArg(args, "--produce-count=", "KAFSCALE_PRODUCE_COUNT", "25"), 25);
            int consumeCount = parseInt(envOrArg(args, "--consume-count=", "KAFSCALE_CONSUME_COUNT", "5"), 5);
            long consumeTimeout = parseLong(envOrArg(args, "--consume-timeout-ms=", "KAFSCALE_CONSUME_TIMEOUT_MS", "10000"), 10000);
            String acks = envOrArg(args, "--acks=", "KAFSCALE_ACKS", "0");
            boolean idempotence = Boolean.parseBoolean(envOrArg(args, "--idempotence=", "KAFSCALE_ENABLE_IDEMPOTENCE", "false"));
            int retries = parseInt(envOrArg(args, "--retries=", "KAFSCALE_RETRIES", "3"), 3);
            int timeoutMs = parseInt(envOrArg(args, "--timeout-ms=", "KAFSCALE_TIMEOUT_MS", "2000"), 2000);
            String groupId = envOrArg(args, "--group-id=", "KAFSCALE_GROUP_ID",
                    "simple-demo-group-" + UUID.randomUUID());
            return new Config(bootstrap, topic, produceCount, consumeCount, consumeTimeout, acks, idempotence, retries, timeoutMs, groupId);
        }

        private static String envOrArg(String[] args, String prefix, String envKey, String fallback) {
            String envValue = System.getenv(envKey);
            if (envValue != null && !envValue.isBlank()) {
                return envValue.trim();
            }
            for (String arg : args) {
                if (arg != null && arg.startsWith(prefix)) {
                    return arg.substring(prefix.length()).trim();
                }
            }
            return fallback;
        }

        private static int parseInt(String value, int fallback) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                return fallback;
            }
        }

        private static long parseLong(String value, long fallback) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException ex) {
                return fallback;
            }
        }
    }
}
