package com.example.kafkaconsumer.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.Properties;

import org.springframework.stereotype.Component;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class SpecificOffsetCommitKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>(); // 1
  private long count = 0;

  public SpecificOffsetCommitKafkaConsumer() {
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
    try{
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
            
          currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));

          if (count % 100 == 0) {
            consumer.commitAsync(currentOffsets, null);
          }
          count++;
        }
      }
    } catch(WakeupException e) {
      log.info("SpecificOffsetCommitKafkaConsumer wakeup exception");
      // ignore for shutdown
    } finally {
      consumer.close();
      log.info("SpecificOffsetCommitKafkaConsumer closed");
    }
  }

  @Override
  public void wakeup() {
    log.info("SpecificOffsetCommitKafkaConsumer wakeup");
    consumer.wakeup();
  }
}