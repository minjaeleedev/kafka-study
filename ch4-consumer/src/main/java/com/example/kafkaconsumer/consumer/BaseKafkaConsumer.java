package com.example.kafkaconsumer.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BaseKafkaConsumer {
  private final KafkaConsumer<String, String> consumer;

  public BaseKafkaConsumer() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    this.consumer = new KafkaConsumer<>(kafkaProps);
  }

  public void subscribe(String topic) {
    consumer.subscribe(Collections.singletonList(topic));
  }
}
