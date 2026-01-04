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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducerService {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;
    private final String topic;

    public OrderProducerService(
            KafkaTemplate<String, Order> kafkaTemplate,
            @Value("${app.kafka.topic}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendOrder(Order order) {
        logger.info("Sending order to KafScale: {}", order);
        kafkaTemplate.send(topic, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        logger.info("Order sent successfully: {} to partition {}",
                                order.getOrderId(),
                                result.getRecordMetadata().partition());
                    } else {
                        logger.error("Failed to send order: {}", order.getOrderId(), ex);
                    }
                });
    }
}
