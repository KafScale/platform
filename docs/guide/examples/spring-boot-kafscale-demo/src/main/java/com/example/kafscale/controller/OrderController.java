package com.example.kafscale.controller;

import com.example.kafscale.model.Order;
import com.example.kafscale.service.OrderProducerService;
import com.example.kafscale.service.OrderConsumerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService producerService;
    private final OrderConsumerService consumerService;
    private final org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties;
    private final org.springframework.kafka.core.KafkaAdmin kafkaAdmin;

    public OrderController(OrderProducerService producerService, OrderConsumerService consumerService,
            org.springframework.boot.autoconfigure.kafka.KafkaProperties kafkaProperties,
            org.springframework.kafka.core.KafkaAdmin kafkaAdmin) {
        this.producerService = producerService;
        this.consumerService = consumerService;
        this.kafkaProperties = kafkaProperties;
        this.kafkaAdmin = kafkaAdmin;
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
        return ResponseEntity.ok(config);
    }
}
