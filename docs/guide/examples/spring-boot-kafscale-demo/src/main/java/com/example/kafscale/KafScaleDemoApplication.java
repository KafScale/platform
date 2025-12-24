package com.example.kafscale;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class KafScaleDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafScaleDemoApplication.class, args);
    }
}
