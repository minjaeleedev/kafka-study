package com.example.kafkaproducer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class SynchronousRawKafkaProducer {
    private final KafkaProducer<String, String> producer;

    public SynchronousRawKafkaProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        this.producer = new KafkaProducer<>(kafkaProps);
    }

    public void send(String topic, String key, String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
        try {
          // send synchronous
          log.info("Sent message to topic: {}", record.toString());
          producer.send(record).get();
        } catch(Exception e) {
          e.printStackTrace();
        }
    }

    @PreDestroy
    public void close() {
        producer.close();
    }
}
