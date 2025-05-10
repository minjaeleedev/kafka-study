package com.example.kafkaconsumer.consumer;

import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

import org.json.JSONObject;
import org.springframework.stereotype.Component;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.HashMap;

@Component
public class BaseKafkaConsumer {
  private final KafkaConsumer<String, String> consumer;
  private final Map<String, Integer> custCountryMap = new HashMap<>();

  public BaseKafkaConsumer() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
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
        System.out.printf("topic = %s, partition = %d, offset = %d, " + "customer = %s, country = %s\n",
          record.topic(),
          record.partition(),
          record.offset(),
          record.key(),
          record.value()
        );
        int updateCount = 0;
        if (custCountryMap.containsKey(record.key())) {
          updateCount = custCountryMap.get(record.key()) + 1;
        }
        custCountryMap.put(record.key(), updateCount);
        JSONObject jsonObject = new JSONObject(this.custCountryMap);

        System.out.println(jsonObject.toString());
      }
    }
  }
}
