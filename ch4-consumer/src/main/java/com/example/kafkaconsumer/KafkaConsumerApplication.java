package com.example.kafkaconsumer;

import java.util.Collections;
import java.util.regex.Pattern;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.kafkaconsumer.consumer.BaseKafkaConsumer;


@SpringBootApplication
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        ApplicationContext context = SpringApplication.run(KafkaConsumerApplication.class, args);
        BaseKafkaConsumer kafkaConsumer = context.getBean(BaseKafkaConsumer.class);
        // subscribe by topic
        kafkaConsumer.subscribe(Collections.singletonList("customerCountries"));
        // subscribe by regex
        kafkaConsumer.subscribe(Pattern.compile("test.*"));

        kafkaConsumer.poll();
    }
}