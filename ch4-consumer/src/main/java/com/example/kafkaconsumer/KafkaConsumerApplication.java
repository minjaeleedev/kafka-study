package com.example.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.kafkaconsumer.consumer.BaseKafkaConsumer;

@SpringBootApplication
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(KafkaConsumerApplication.class, args);
        BaseKafkaConsumer rawKafkaConsumer = context.getBean(BaseKafkaConsumer.class);
        rawKafkaConsumer.subscribe("customerCountries");
    }
}