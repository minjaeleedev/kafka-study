package com.example.kafkaproducer.controller;

public class KafkaMessageDto {
  private String key;
  private String message;

  public String getKey() {
    return key;
  }

  public String getMessage() {
    return message;
  }
}
