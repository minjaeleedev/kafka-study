package com.example.kafkaconsumer.repository;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.HashMap;

import org.springframework.stereotype.Repository;

@Repository
public class CustomerCountryRepository {
  private final ConcurrentHashMap<String, Integer> custCountryMap = new ConcurrentHashMap<>();

  public void update(String customer) {
    int updateCount = 0;
    if (custCountryMap.containsKey(customer)) {
      updateCount = custCountryMap.get(customer) + 1;
    }
    custCountryMap.put(customer, updateCount);
  }

  public Map<String, Integer> getCustomerCountryMap() {
    return new HashMap<>(custCountryMap);
  }
}
