package com.example.kafkaconsumer.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.regex.Pattern;
import java.util.Properties;

import org.springframework.stereotype.Component;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AsyncSyncCommitKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private volatile boolean closing = false;

  public AsyncSyncCommitKafkaConsumer() {
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
    Duration timeout = Duration.ofMillis(100);

    try {
      while (!closing) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        consumer.commitAsync(); // 1
      }
      consumer.commitSync(); // 2
    } catch (Exception e) {
      log.error("Commit failed", e);
    } finally {
      consumer.close();
    }
  }

  @Override
  public void wakeup() {
    closing = true;
  }
}
