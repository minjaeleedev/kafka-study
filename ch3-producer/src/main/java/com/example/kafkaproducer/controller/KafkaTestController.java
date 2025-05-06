package com.example.kafkaproducer.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;

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
    public void sendSync() {
        syncProducer.send("test-topic", "sync-key", "hello sync");
    }

    @PostMapping("/async")
    public void sendAsync() {
        asyncProducer.send("test-topic", "async-key", "hello async");
    }
}
