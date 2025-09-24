package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class LogProcessor {
    private static final ConcurrentHashMap<String, AtomicInteger> endpointCounts = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, AtomicInteger> errorCounts = new ConcurrentHashMap<>();
    private static final ExecutorService pool = Executors.newFixedThreadPool(5);

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "log-processor");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(List.of("raw-logs"));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Print metrics every 30 seconds
        ScheduledExecutorService metricsExporter = Executors.newScheduledThreadPool(1);
        metricsExporter.scheduleAtFixedRate(LogProcessor::printMetrics, 0, 30, TimeUnit.SECONDS);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                pool.submit(() -> processLog(record.value(), producer));
            }
        }
    }

    private static void processLog(String log, KafkaProducer<String, String> producer) {
    try {
        String regex = "\\[(.*?)\\] (INFO|ERROR) (\\S+) (.*)";
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(regex).matcher(log);

        if (m.matches()) {
            String timestamp = m.group(1);
            String level = m.group(2);
            String endpoint = m.group(3);
            String message = m.group(4);

            endpointCounts.computeIfAbsent(endpoint, k -> new AtomicInteger()).incrementAndGet();
            if ("ERROR".equals(level)) {
                errorCounts.computeIfAbsent(endpoint, k -> new AtomicInteger()).incrementAndGet();
            }

            String json = String.format(
                "{\"timestamp\":\"%s\",\"level\":\"%s\",\"endpoint\":\"%s\",\"message\":\"%s\"}",
                timestamp, level, endpoint, message
            );
            producer.send(new ProducerRecord<>("parsed-logs", json));
        } else {
            System.err.println("Failed to parse log: " + log);
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
}

    private static void printMetrics() {
        System.out.println("==== Metrics Snapshot ====");
        endpointCounts.forEach((endpoint, total) -> {
            int errors = errorCounts.getOrDefault(endpoint, new AtomicInteger(0)).get();
            double errorRate = total.get() > 0 ? (double) errors / total.get() : 0;
            System.out.printf("Endpoint: %s | Total: %d | Errors: %d | ErrorRate: %.2f%%\n",
                    endpoint, total.get(), errors, errorRate * 100);
        });
        System.out.println("==========================");
    }
}
