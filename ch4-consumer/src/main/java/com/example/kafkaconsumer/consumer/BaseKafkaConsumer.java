package com.example.kafkaconsumer.consumer;

import java.util.Collection;
import java.util.Properties;
import java.util.regex.Pattern;

import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.example.kafkaconsumer.repository.CustomerCountryRepository;

import lombok.extern.slf4j.Slf4j;


@Component
@Slf4j
public class BaseKafkaConsumer implements KafkaConsumerWorker {
  private final KafkaConsumer<String, String> consumer;
  private final CustomerCountryRepository customerCountryRepository;

  @Autowired
  public BaseKafkaConsumer(CustomerCountryRepository customerCountryRepository) {
    this.customerCountryRepository = customerCountryRepository;
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

          String customer = record.key();
          customerCountryRepository.update(customer);
          JSONObject jsonObject = new JSONObject(this.customerCountryRepository.getCustomerCountryMap());

          log.info(jsonObject.toString());
        }
      }
    } catch (WakeupException e) {
      log.info("BaseKafkaConsumer wakeup exception");
      // ignore for shutdown
    } finally {
      consumer.close();
      log.info("BaseKafkaConsumer closed");
    }
  }

  public void wakeup() {
    log.info("BaseKafkaConsumer shutdown");
    consumer.wakeup();
  }
  
  @Override
  public String getTopic() {
    return "customerCountries";
  }
}
