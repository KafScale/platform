package com.example.kafscale.service;

import com.example.kafscale.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(OrderConsumerService.class);

    private final java.util.List<Order> receivedOrders = java.util.Collections
            .synchronizedList(new java.util.ArrayList<>());

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeOrder(Order order) {
        logger.info("Received order from KafScale: {}", order);
        receivedOrders.add(0, order); // Add to beginning of list
        if (receivedOrders.size() > 50) {
            receivedOrders.remove(receivedOrders.size() - 1); // Keep last 50
        }
        processOrder(order);
    }

    private void processOrder(Order order) {
        // Simulate order processing
        logger.info("Processing order: {} for product: {} (quantity: {})",
                order.getOrderId(), order.getProduct(), order.getQuantity());
    }

    public java.util.List<Order> getReceivedOrders() {
        return new java.util.ArrayList<>(receivedOrders);
    }
}
