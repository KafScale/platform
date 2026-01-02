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

public class SimpleDemo {
    private static final Logger logger = LoggerFactory.getLogger(SimpleDemo.class);
    private static final String TOPIC_NAME = "demo-topic-1";
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static void main(String[] args) {

        logger.info("Starting SimpleDemo...");

        // 0. Show all topics
        try {
            showTopics();
        } catch (Exception e) {
            logger.warn("Skipping showTopics due to error: {}", e.getMessage());
        }

        // 1. Ensure topic exists
        try {
            createTopic();
        } catch (Exception e) {
            logger.warn("Skipping createTopic due to error: {}", e.getMessage());
        }

        // 2. Produce 5 messages
        produceMessages();

        // 3. Show Cluster Metadata
        try {
            showClusterMetadata();
        } catch (Exception e) {
            logger.warn("Skipping showClusterMetadata due to error: {}", e.getMessage());
        }

        // 3. Consume 5 messages
        consumeMessages();

    }

    private static void createTopic() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", "2000");
        props.put("default.api.timeout.ms", "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Check if topic exists
            boolean topicExists = adminClient.listTopics().names().get(5, TimeUnit.SECONDS).contains(TOPIC_NAME);

            if (!topicExists) {
                logger.info("Topic {} does not exist. Creating it...", TOPIC_NAME);
                NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get(5, TimeUnit.SECONDS);
                logger.info("Topic {} created successfully.", TOPIC_NAME);
            } else {
                logger.info("Topic {} already exists.", TOPIC_NAME);
            }
        }
    }

    private static void produceMessages() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // KafScale works best with idempotence disabled for now if issues arise, but
        // default should be fine.
        // Explicitly setting it to false as in the spring boot example just in case.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
        props.put(ProducerConfig.ACKS_CONFIG, "0"); // Match Spring Boot demo
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", "2000");
        props.put("default.api.timeout.ms", "5000");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 25; i++) {
                String key = "key-" + i;
                String value = "message-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

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

    private static void consumeMessages() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "simple-demo-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", "2000");
        props.put("default.api.timeout.ms", "5000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            int messagesReceived = 0;
            // Poll for a limited time to get the 5 messages
            long endTime = System.currentTimeMillis() + 10000; // 10 seconds timeout

            while (messagesReceived < 25 && System.currentTimeMillis() < endTime) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Received message: key={} value={} partition={} offset={}",
                            record.key(), record.value(), record.partition(), record.offset());
                    messagesReceived++;
                    if (messagesReceived >= 5)
                        break;
                }
            }

            if (messagesReceived < 5) {
                logger.warn("Timed out waiting for messages. Received: {}", messagesReceived);
            } else {
                logger.info("Successfully consumed {} messages.", messagesReceived);
            }

        } catch (Exception e) {
            logger.error("Consumer error", e);
        }
    }

    private static void showTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", "2000");
        props.put("default.api.timeout.ms", "5000");

        try (AdminClient adminClient = AdminClient.create(props)) {
            logger.info("Listing topics...");
            adminClient.listTopics().names().get(5, TimeUnit.SECONDS).forEach(name -> logger.info("Topic: {}", name));
        }
    }

    private static void showClusterMetadata() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("request.timeout.ms", "2000");
        props.put("default.api.timeout.ms", "5000");

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
}
