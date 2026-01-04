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
