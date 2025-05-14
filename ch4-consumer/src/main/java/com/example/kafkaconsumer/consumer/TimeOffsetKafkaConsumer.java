package com.example.kafkaconsumer.consumer;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import java.util.Properties;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class TimeOffsetKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  private long count = 0;

  public TimeOffsetKafkaConsumer() {
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
    long oneHourEarlier = Instant.now().atZone(ZoneId.systemDefault())
    .minusHours(1).toEpochSecond() * 1000; // Kafka는 밀리초 단위

    Map<TopicPartition, Long> timestampMap = consumer.assignment().stream()
    .collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));
    Map<TopicPartition, OffsetAndTimestamp> offsetMap = consumer.offsetsForTimes(timestampMap);

    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
      OffsetAndTimestamp offsetAndTimestamp = entry.getValue();
      if (offsetAndTimestamp != null) {
        consumer.seek(entry.getKey(), offsetAndTimestamp.offset());
      }
    }

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
  }

  @Override
  public void wakeup() {
    consumer.close();
  }
}