package com.example.kafkaconsumer;

import java.util.Collections;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

import com.example.kafkaconsumer.consumer.MainThreadKafkaConsumer;

@SpringBootApplication
public class KafkaConsumerApplication {
    public static void main(String[] args) {
        ApplicationContext app = SpringApplication.run(KafkaConsumerApplication.class, args);
        
        MainThreadKafkaConsumer consumer = app.getBean(MainThreadKafkaConsumer.class);
        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down application...");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }));

        consumer.subscribe(Collections.singletonList("customerCountries"));
        consumer.poll();
    }
}