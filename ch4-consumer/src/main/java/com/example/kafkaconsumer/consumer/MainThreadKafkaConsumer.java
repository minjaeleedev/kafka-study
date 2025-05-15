package com.example.kafkaconsumer.consumer;

import java.util.Properties;
import java.util.Collection;
import java.util.regex.Pattern;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class MainThreadKafkaConsumer {
  private final KafkaConsumer<String, String> consumer;

  public MainThreadKafkaConsumer() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    this.consumer = new KafkaConsumer<>(kafkaProps);
  }

  public void subscribe(Collection<String> topics) {
    consumer.subscribe(topics);
  }

  public void subscribe(Pattern pattern) {
    consumer.subscribe(pattern);
  }
  
  public void poll() {
    Duration timeout = Duration.ofMillis(10000);
    try{
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        log.info(System.currentTimeMillis() + "-- waiting for data...");

        for (ConsumerRecord<String, String> record : records) {
          log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }

        for (TopicPartition partition : consumer.assignment()) {
          log.info("committing offset at position {}", consumer.position(partition));
        }

        consumer.commitSync();
      }
    } catch (WakeupException e) {
      // ignore for shutdown
    } finally {
      consumer.close();
      log.info("closed consumer and we are done");
    }
  }

  public void wakeup() {
    consumer.wakeup();
  }

  public String getTopic() {
    return "customerCountries";
  }
}
