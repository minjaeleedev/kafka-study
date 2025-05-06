package com.example.kafkaproducer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

@Component
public class RawKafkaProducer {
    private final KafkaProducer<String, String> producer;

    public RawKafkaProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

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