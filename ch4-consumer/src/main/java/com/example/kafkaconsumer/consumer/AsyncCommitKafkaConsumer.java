package com.example.kafkaconsumer.consumer;

import java.util.Collection;
import java.util.regex.Pattern;
import java.util.Map;

import java.time.Duration;

import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.atomic.AtomicLong;

@Component
@Slf4j
public class AsyncCommitKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private final AtomicLong commitVersion = new AtomicLong(0);

  public AsyncCommitKafkaConsumer() {
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

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(timeout);
      for (ConsumerRecord<String, String> record : records) {
        log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
          record.topic(), record.partition(), record.offset(), record.key(), record.value());
      }

      long version = commitVersion.incrementAndGet();
      consumer.commitAsync(new OffsetCommitCallback() {
        @Override
        public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
          if (exception != null) {
            if(version == commitVersion.get()) {
              // retryCommitAsync(offsets)
            } else{
              log.error("Commit failed for offsets : {}", offsets, exception);
            }
          }
        }
      });
    }
  }

  @Override
  public void shutdown() {
    consumer.close();
  }
}
