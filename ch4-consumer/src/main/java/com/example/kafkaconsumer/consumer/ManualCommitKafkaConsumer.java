package com.example.kafkaconsumer.consumer;

import java.util.Properties;
import java.util.Collection;
import java.util.regex.Pattern;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ManualCommitKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer; 

  public ManualCommitKafkaConsumer() {
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
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value()
          );
        }
        
        try {
          consumer.commitSync();
        } catch (CommitFailedException e) {
          log.error("Commit failed", e);
        }
      }
    } catch (WakeupException e) {
      log.info("ManualCommitKafkaConsumer wakeup exception");
      // ignore for shutdown
    } finally {
      consumer.close();
      log.info("ManualCommitKafkaConsumer closed");
    }
  }

  public void wakeup() {
    log.info("ManualCommitKafkaConsumer shutdown");
    consumer.wakeup();
  }
}