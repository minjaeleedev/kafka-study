package com.example.kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

@Component
public class RawKafkaProducer {
    private final KafkaProducer<String, String> producer;

    public RawKafkaProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "kafka:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(kafkaProps);
    }

    public void send(String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        try {
          producer.send(record);
        } catch(Exception e) {
          e.printStackTrace();
        }
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}