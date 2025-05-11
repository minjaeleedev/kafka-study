package com.example.kafkaproducer.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.http.ResponseEntity;

import com.example.kafkaproducer.producer.AsynchronousRawKafkaProducer;
import com.example.kafkaproducer.producer.SynchronousRawKafkaProducer;

import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/produce")
@RequiredArgsConstructor
public class KafkaTestController {
    private final SynchronousRawKafkaProducer syncProducer;
    private final AsynchronousRawKafkaProducer asyncProducer;

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
}
