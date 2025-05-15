package com.example.kafkaconsumer.deserializer;

import com.example.kafkaconsumer.domain.Customer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.errors.SerializationException;

import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> { // 1
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  public Customer deserialize(String topic, byte[] data) {
    int id;
    int nameSize;
    String name;

    try {
      if (data==null) {
        return null;
      }

      if (data.length < 8) {
        throw new SerializationException("Size of data received " + 
          "by deserializer is shorter than expected");
      }

      ByteBuffer buffer = ByteBuffer.wrap(data);
      id = buffer.getInt();
      nameSize = buffer.getInt();

      byte[] nameBytes = new byte[nameSize];
      buffer.get(nameBytes);
      name = new String(nameBytes, "UTF-8");

      return new Customer(id, name); // 2
    } catch (Exception e) {
      throw new SerializationException("Error when deserializing " + 
          "byte[] to Customer: " + e);
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
