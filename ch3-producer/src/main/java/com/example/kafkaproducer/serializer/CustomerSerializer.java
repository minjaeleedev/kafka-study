package com.example.kafkaproducer.serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.errors.SerializationException;
import com.example.kafkaproducer.domain.Customer;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomerSerializer implements Serializer<Customer> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // nothing to configure
  }

  @Override
  /**
    We are serializing Customer as:
    - 4 bytes int representing customerId
    - 4 bytes int representing length of customerName in UTF-8 bytes (0 if name is Null)
    N bytes representing customerName in UTF-8
   **/
  public byte[] serialize(String topic, Customer data) {
    try{
      byte[] serializedName;
      int stringSize;
      if (data == null) {
        return null;
      } else {
        if (data.getName() != null) {
          serializedName = data.getName().getBytes("UTF-8");
          stringSize = serializedName.length;
        } else{
          serializedName = new byte[0];
          stringSize = 0;
        }
      }
      
      ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + stringSize);
      buffer.putInt(data.getId());
      buffer.putInt(stringSize);
      buffer.put(serializedName);
      return buffer.array();
    } catch(Exception e) {
      throw new SerializationException("Error when serializing Customer to byte[]");
    }
  }

  @Override
  public void close() {
    // nothing to close
  }
}
