package com.example.kafkaproducer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.kafkaproducer.producer.RawKafkaProducer;

@SpringBootApplication
public class KafkaProducerApplication {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(KafkaProducerApplication.class, args);
        RawKafkaProducer rawKafkaProducer = context.getBean(RawKafkaProducer.class);
        // if auto.create.topics.enable is true, the topic will be created automatically
        rawKafkaProducer.send("customerCountries", "Precision Products", "France");
    }
}