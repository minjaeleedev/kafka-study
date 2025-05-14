package com.example.kafkaconsumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import java.util.Collections;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import com.example.kafkaconsumer.consumer.KafkaConsumerWorker;

@Component
@Slf4j
public class KafkaConsumerRunner {
    private final List<KafkaConsumerWorker> consumers;
    private final ExecutorService executorService = Executors.newFixedThreadPool(7);

    public KafkaConsumerRunner(List<KafkaConsumerWorker> consumers) {
        this.consumers = consumers;
    }

    @PostConstruct
    public void startConsumers() {
        for (KafkaConsumerWorker consumer : consumers) {
            log.info("Starting consumer : {}", consumer.getClass().getSimpleName());
            consumer.subscribe(Collections.singletonList("customerCountries"));
            executorService.submit(consumer::poll);
        }
    }

    @PreDestroy
    public void shutdown() {
        for (KafkaConsumerWorker consumer : consumers) {
            log.info("Shutting down consumer : {}", consumer.getClass().getSimpleName());
            consumer.shutdown(); // ✅ 종료 요청
        }
        executorService.shutdownNow();
    }
}
