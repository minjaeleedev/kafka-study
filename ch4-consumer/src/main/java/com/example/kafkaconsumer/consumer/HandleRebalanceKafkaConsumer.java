package com.example.kafkaconsumer.consumer;

import java.time.Duration;
import java.util.regex.Pattern;
import java.util.Collection;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class HandleRebalanceKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

  public HandleRebalanceKafkaConsumer() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "CountryCounter");
    kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    this.consumer = new KafkaConsumer<>(kafkaProps);
  }

  private class HandleRebalance implements ConsumerRebalanceListener { // 1
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      // 2
      // do nothing
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      log.info("Lost partitions in rebalance. " + 
        "Committing current offsets" + currentOffsets);
      consumer.commitSync(currentOffsets); // 3. 모든 파티션에 대해 커밋
    }
  }

  public void subscribe(Collection<String> topics) {
    consumer.subscribe(topics, new HandleRebalance()); // 4
  }

  public void subscribe(Pattern pattern) {
    consumer.subscribe(pattern, new HandleRebalance());
  }

  public void poll() {
    try {
      while(true) {
        Duration timeout = Duration.ofMillis(100);
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          log.info("topic = {}, partition = {}, offset = {}, customer = {}, country = {}",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
          currentOffsets.put(
            new TopicPartition(record.topic(), record.partition()), 
            new OffsetAndMetadata(record.offset()+1, null));
          consumer.commitAsync(currentOffsets, null);
        }
      }
    } catch(WakeupException e) {
      log.info("WakeupException");
    } catch(Exception e) {
      log.error("Unexpected error", e);
    } finally {
      consumer.close();
      log.info("Closed consumer and we are done");
    }
  }

  @Override
  public void wakeup() {}
}
