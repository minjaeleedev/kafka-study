package com.example.kafkaproducer.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.http.ResponseEntity;

import com.example.kafkaproducer.producer.AsynchronousRawKafkaProducer;
import com.example.kafkaproducer.producer.SynchronousRawKafkaProducer;
import com.example.kafkaproducer.producer.CustomerSerializerKafkaProducer;
import com.example.kafkaproducer.domain.Customer;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class KafkaTestController {
    private final SynchronousRawKafkaProducer syncProducer;
    private final AsynchronousRawKafkaProducer asyncProducer;
    private final CustomerSerializerKafkaProducer customerSerializerProducer;

    @PostMapping("/sync")
    public ResponseEntity<String> sendSync() {
        syncProducer.send("customerCountries", "sync-key", "hello sync");
        return ResponseEntity.ok("Sync message sent");
    }

    @PostMapping("/async")
    public ResponseEntity<String> sendAsync() {
        asyncProducer.send("customerCountries", "async-key", "hello async");
        return ResponseEntity.ok("Async message sent");
    }

    @PostMapping("/topic/customer")
    public ResponseEntity<String> sendCustomer() {  
        Customer customer = new Customer(1, "John");
        customerSerializerProducer.send("customerSerializer", "customer-key", customer);
        return ResponseEntity.ok("Customer message sent");
    }

    @PostMapping("/topic/standalone")
    public ResponseEntity<String> sendStandalone() {
        syncProducer.send("standalone", "standalone-key", "hello standalone");
        return ResponseEntity.ok("Standalone message sent");
    }
}
