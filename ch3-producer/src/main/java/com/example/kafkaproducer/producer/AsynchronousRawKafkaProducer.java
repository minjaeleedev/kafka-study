package com.example.kafkaproducer.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import jakarta.annotation.PreDestroy;
import java.util.Properties;

@Component
public class AsynchronousRawKafkaProducer {
  private final KafkaProducer<String, String> producer;
  private class DemoProducerCallback implements Callback {
    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null) {
        exception.printStackTrace();
      } 
    }
  }

  public AsynchronousRawKafkaProducer() {
      Properties kafkaProps = new Properties();
      kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
      kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
      kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

      this.producer = new KafkaProducer<>(kafkaProps);
  }

  public void send(String topic, String key, String message) {
      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, message);
      try {
        // send asynchronous
        producer.send(record, new DemoProducerCallback());
      } catch(Exception e) {
        e.printStackTrace();
      }
  }

  @PreDestroy
  public void close() {
      producer.close();
  }
}