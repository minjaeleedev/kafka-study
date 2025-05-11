package com.example.kafkaconsumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Collections;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import com.example.kafkaconsumer.consumer.KafkaConsumerWorker;

public class KafkaConsumerRunner {
    private final List<KafkaConsumerWorker> consumers;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);

    public KafkaConsumerRunner(List<KafkaConsumerWorker> consumers) {
        this.consumers = consumers;
    }

    @PostConstruct
    public void startConsumers() {
        for (KafkaConsumerWorker consumer : consumers) {
            consumer.subscribe(Collections.singletonList("customerCountries"));
            executorService.submit(consumer::poll);
        }
    }

    @PreDestroy
    public void shutdown() {
        executorService.shutdownNow();
    }
}
