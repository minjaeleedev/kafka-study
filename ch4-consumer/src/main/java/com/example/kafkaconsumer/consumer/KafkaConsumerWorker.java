package com.example.kafkaconsumer.consumer;

import java.util.Collection;
import java.util.regex.Pattern;

public interface KafkaConsumerWorker {
  void subscribe(Collection<String> topics);
  void subscribe(Pattern pattern);
  void poll();
}