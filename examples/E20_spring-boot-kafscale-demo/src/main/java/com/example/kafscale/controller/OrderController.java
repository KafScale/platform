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

package com.example.kafscale.controller;

import com.example.kafscale.model.Order;
import com.example.kafscale.service.OrderProducerService;
import com.example.kafscale.service.OrderConsumerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.core.env.Environment;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;
    private final OrderConsumerService consumerService;
    private final org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;
    private final org.springframework.kafka.core.KafkaAdmin kafkaAdmin;
    private final Environment env;

    public OrderController(OrderProducerService producerService, OrderConsumerService consumerService,
            org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties,
            org.springframework.kafka.core.KafkaAdmin kafkaAdmin, Environment env) {
        this.producerService = producerService;
        this.consumerService = consumerService;
        this.kafkaProperties = kafkaProperties;
        this.kafkaAdmin = kafkaAdmin;
        this.env = env;
    }

    @PostMapping("/test-connection")
    public ResponseEntity<String> testConnection() {
        try {
            try (org.apache.kafka.clients.admin.AdminClient client = org.apache.kafka.clients.admin.AdminClient
                    .create(kafkaAdmin.getConfigurationProperties())) {
                client.listTopics().names().get(5, java.util.concurrent.TimeUnit.SECONDS);
                return ResponseEntity.ok("Connected");
            }
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Failed: " + e.getMessage());
        }
    }

    @PostMapping
    public ResponseEntity<String> createOrder(@RequestBody Order order) {
        try {
            // Generate order ID if not provided
            if (order.getOrderId() == null || order.getOrderId().isEmpty()) {
                order.setOrderId(UUID.randomUUID().toString());
            }

            producerService.sendOrder(order);
            return ResponseEntity.ok("Order sent: " + order.getOrderId());
        } catch (Exception e) {
            return ResponseEntity.status(500).body("Error: " + e.getMessage());
        }
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }

    @GetMapping
    public ResponseEntity<java.util.List<Order>> getOrders() {
        return ResponseEntity.ok(consumerService.getReceivedOrders());
    }

    @GetMapping("/config")
    public ResponseEntity<java.util.Map<String, Object>> getConfig() {
        java.util.Map<String, Object> config = new java.util.HashMap<>();
        config.put("bootstrapServers", kafkaProperties.getBootstrapServers());
        config.put("producer", kafkaProperties.getProducer().getProperties());
        config.put("consumer", kafkaProperties.getConsumer().getProperties());
        config.put("template", kafkaProperties.getTemplate());
        config.put("properties", kafkaProperties.getProperties());
        // Add specific key fields for visibility
        config.put("producerArgs", kafkaProperties.getProducer());
        config.put("consumerArgs", kafkaProperties.getConsumer());
        config.put("activeProfiles", env.getActiveProfiles());
        return ResponseEntity.ok(config);
    }

    @GetMapping("/cluster-info")
    public ResponseEntity<java.util.Map<String, Object>> getClusterInfo() {
        java.util.Map<String, Object> info = new java.util.HashMap<>();
        try (org.apache.kafka.clients.admin.AdminClient client = org.apache.kafka.clients.admin.AdminClient
                .create(kafkaAdmin.getConfigurationProperties())) {

            org.apache.kafka.clients.admin.DescribeClusterResult cluster = client.describeCluster();

            info.put("clusterId", cluster.clusterId().get());
            info.put("controller", mapNode(cluster.controller().get()));

            java.util.List<java.util.Map<String, Object>> nodeList = new java.util.ArrayList<>();
            for (org.apache.kafka.common.Node node : cluster.nodes().get()) {
                nodeList.add(mapNode(node));
            }
            info.put("nodes", nodeList);

            // Fetch topics
            java.util.Set<String> topics = client.listTopics().names().get();
            info.put("topics", topics);

            return ResponseEntity.ok(info);
        } catch (Exception e) {
            info.put("error", e.getMessage());
            return ResponseEntity.status(500).body(info);
        }
    }

    private java.util.Map<String, Object> mapNode(org.apache.kafka.common.Node node) {
        java.util.Map<String, Object> map = new java.util.HashMap<>();
        if (node != null) {
            map.put("id", node.id());
            map.put("idString", node.idString());
            map.put("host", node.host());
            map.put("port", node.port());
            map.put("rack", node.rack());
        }
        return map;
    }
}
