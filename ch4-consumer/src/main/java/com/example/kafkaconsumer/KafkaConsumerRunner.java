package com.example.kafkaconsumer;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;

import com.example.kafkaconsumer.consumer.KafkaConsumerWorker;
import com.example.kafkaconsumer.consumer.StandaloneKafkaConsumer;
@Component
@Slf4j
public class KafkaConsumerRunner {
    private final List<KafkaConsumerWorker> consumers;
    private final StandaloneKafkaConsumer standaloneConsumer;
    private final ExecutorService executorService;

    public KafkaConsumerRunner(List<KafkaConsumerWorker> consumers, StandaloneKafkaConsumer standaloneConsumer) {
        this.consumers = consumers;
        this.standaloneConsumer = standaloneConsumer;
        this.executorService = Executors.newFixedThreadPool(consumers.size() + 1);
    }

    @PostConstruct
    public void startConsumers() {
        for (KafkaConsumerWorker consumer : consumers) {
            String className = consumer.getClass().getSimpleName();
            log.info("Starting consumer : {}", className);
            consumer.subscribe(Collections.singletonList(consumer.getTopic()));
            executorService.submit(consumer::poll);
        }

        executorService.submit(standaloneConsumer::poll);
    }

    @PreDestroy
    public void shutdown() {
        for (KafkaConsumerWorker consumer : consumers) {
            log.info("Shutting down consumer : {}", consumer.getClass().getSimpleName());
            consumer.wakeup(); // 종료 요청
        }
        executorService.shutdown();
        
        try {
            executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Error shutting down executor service", e);
        }
    }
}
