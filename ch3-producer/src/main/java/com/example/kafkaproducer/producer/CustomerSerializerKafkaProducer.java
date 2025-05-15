package com.example.kafkaproducer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

import com.example.kafkaproducer.domain.Customer;
import com.example.kafkaproducer.serializer.CustomerSerializer;

@Component
public class CustomerSerializerKafkaProducer {
    private final KafkaProducer<String, Customer> producer;

    public CustomerSerializerKafkaProducer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class.getName());

        this.producer = new KafkaProducer<>(kafkaProps);
    }

    public void send(String topic, String key, Customer message) {
        ProducerRecord<String, Customer> record = new ProducerRecord<>(topic, key, message);
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
