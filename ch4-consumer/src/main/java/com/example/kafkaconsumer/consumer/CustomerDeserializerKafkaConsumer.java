package com.example.kafkaconsumer.consumer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import com.example.kafkaconsumer.deserializer.CustomerDeserializer;
import com.example.kafkaconsumer.domain.Customer;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class CustomerDeserializerKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, Customer> consumer;

  public CustomerDeserializerKafkaConsumer() {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092"); 
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-group");  
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, "CustomerDeserializerKafkaConsumer");
    this.consumer = new KafkaConsumer<>(props);
  }

  @Override
  public void subscribe(Collection<String> topics) {
    consumer.subscribe(topics);
  }

  @Override
  public void subscribe(Pattern pattern) {
    consumer.subscribe(pattern);
  }

  @Override
  public void poll() {
    Duration timeout = Duration.ofMillis(100);
    
    try {
      while (true) {
        ConsumerRecords<String, Customer> records = consumer.poll(timeout);

        for (ConsumerRecord<String, Customer> record : records) {
          log.info("current customer id: {}, name: {}", record.value().getId(), record.value().getName());
        }

        consumer.commitSync();
      }
    } catch (WakeupException e) {
      log.info("Wakeup from consumer");
    } catch (Exception e) {
      log.error("customer deserializer consumer error", e);
    } finally {
      consumer.close();
      log.info("closed CustomerDeserializerKafkaConsumer and we are done");
    }
  }
  
  @Override
  public void wakeup() {
    consumer.wakeup();
  }

  @Override
  public String getTopic() {
    return "customerSerializer";
  }
}