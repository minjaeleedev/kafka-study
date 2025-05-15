package com.example.kafkaconsumer.consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.time.Duration;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;


import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

@Component
@Slf4j
public class StandaloneKafkaConsumer {
  private final KafkaConsumer<String, String> consumer;

  public StandaloneKafkaConsumer() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "StandaloneConsumer");
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(ConsumerConfig.CLIENT_ID_CONFIG, "StandaloneKafkaConsumer");
    this.consumer = new KafkaConsumer<>(kafkaProps);
  }

  public void poll() {
    Duration timeout = Duration.ofMillis(100);
    ArrayList<TopicPartition> partitions = new ArrayList<>();
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(getTopic());
    if (partitionInfos != null) {
      for (PartitionInfo partition : partitionInfos) {
        partitions.add(new TopicPartition(getTopic(), partition.partition()));
      }

      consumer.assign(partitions);

      try {
        while (true) {
          ConsumerRecords<String, String> records = consumer.poll(timeout);
          for (ConsumerRecord<String, String> record : records) {
            log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
          }

          consumer.commitSync();
        }
      } catch (WakeupException e) {
        log.info("StandaloneKafkaConsumer wakeup exception");
        // ignore for shutdown
      } finally {
        consumer.close();
        log.info("StandaloneKafkaConsumer closed");
      }
    }
  }

  public void wakeup() {
    log.info("StandaloneKafkaConsumer shutdown");
    consumer.wakeup();
  }

  public String getTopic() {
    return "standalone";
  }
}
