package com.example.kafkaconsumer.domain;

public class Customer {
  private int customerId;
  private String customerName;

  public Customer(int customerId, String customerName) {
    this.customerId = customerId;
    this.customerName = customerName;
  }

  public int getId() {
    return customerId;
  }

  public String getName() {
    return customerName;
  }
}
