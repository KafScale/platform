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
