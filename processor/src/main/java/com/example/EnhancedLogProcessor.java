package com.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class EnhancedLogProcessor {

    // Config (can be overridden via env)
    private static final String BROKERS   = envOr("KAFKA_BROKERS", "localhost:9092");
    private static final String RAW_TOPIC = envOr("RAW_TOPIC", "raw-logs");
    private static final String OUT_TOPIC = envOr("OUT_TOPIC", "parsed-logs");
    private static final String DLQ_TOPIC = envOr("DLQ_TOPIC", "dead-logs");
    private static final int    POLL_MS   = Integer.parseInt(envOr("POLL_MS", "200"));
    private static final int    WORKERS   = Integer.parseInt(envOr("WORKERS", "8"));

    // Filtering
    private static final Set<String> ALLOW_LEVELS = Arrays.stream(envOr("ALLOW_LEVELS", "INFO,WARN,ERROR")
            .split(",")).map(String::trim).collect(Collectors.toSet());
    private static final int INFO_SAMPLE_N = Integer.parseInt(envOr("INFO_SAMPLE_N", "1"));

    // Concurrency plumbing
    private static final ExecutorService workers = Executors.newFixedThreadPool(WORKERS, r -> {
        Thread t = new Thread(r);
        t.setName("worker-" + t.getId());
        t.setDaemon(false);
        return t;
    });

    private static final BlockingQueue<ConsumerRecord<String, String>> queue =
            new ArrayBlockingQueue<>(10_000);

    private static final Pattern LOG_PATTERN = Pattern.compile("\\[(.*?)\\]\\s+(DEBUG|INFO|WARN|ERROR)\\s+(\\S+)\\s+(.*)");
    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT.withZone(ZoneOffset.UTC);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    // stats
    private static final AtomicLong received = new AtomicLong();
    private static final AtomicLong forwarded = new AtomicLong();
    private static final AtomicLong dropped = new AtomicLong();
    private static final AtomicLong dlq = new AtomicLong();

    private static volatile boolean running = true;

    public static void main(String[] args) {
        // Producer for OUT + DLQ
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());

        // Start worker threads
        for (int i = 0; i < WORKERS; i++) {
            workers.submit(() -> workerLoop(producer));
        }

        // Periodic stats
        ScheduledExecutorService sched = Executors.newSingleThreadScheduledExecutor();
        sched.scheduleAtFixedRate(() -> {
            System.out.printf(
                    "[Processor] recv=%d fwd=%d drop=%d dlq=%d lag=%d q=%d%n",
                    received.get(), forwarded.get(), dropped.get(), dlq.get(), // simple counters
                    0L, queue.size()
            );
        }, 5, 30, TimeUnit.SECONDS);

        // Consumer thread
        Thread consumerThread = new Thread(() -> runConsumer());
        consumerThread.setName("kafka-consumer");
        consumerThread.start();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                consumerThread.join(2000);
            } catch (InterruptedException ignored) {}
            workers.shutdown();
            sched.shutdown();
            producer.flush();
            producer.close();
        }));
    }

    private static void runConsumer() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps());
        consumer.subscribe(List.of(RAW_TOPIC));

        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(POLL_MS));
                for (ConsumerRecord<String, String> rec : records) {
                    // backpressure: if queue is full, put() blocks
                    try {
                        queue.put(rec);
                        received.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                // We can change this to commitSync() to ensure processing
                consumer.commitAsync();
            }
        } catch (WakeupException ignored) {
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {}
            consumer.close();
        }
    }

    private static void workerLoop(KafkaProducer<String, String> producer) {
        while (running) {
            try {
                ConsumerRecord<String, String> rec = queue.poll(200, TimeUnit.MILLISECONDS);
                if (rec == null) continue;
                processOne(rec.value(), producer);
            } catch (Exception e) {
                // last-resort logging; avoid bringing down thread
                System.err.println("[Worker] Unhandled: " + e.getMessage());
            }
        }
    }

    private static void processOne(String line, KafkaProducer<String, String> producer) {
        Matcher m = LOG_PATTERN.matcher(line);
        if (!m.matches()) {
            sendDlq(producer, "PARSE_ERROR", "No regex match", line);
            return;
        }
        String originalTs = m.group(1);           
        String level      = m.group(2);       
        String endpoint   = m.group(3);        
        String message    = m.group(4);        

        // Filtering
        if (!ALLOW_LEVELS.contains(level)) {
            dropped.incrementAndGet();
            return;
        }
        if ("INFO".equals(level) && INFO_SAMPLE_N > 1) {
            long hash = Math.abs(Objects.hash(line));
            if ((hash % INFO_SAMPLE_N) != 0) {
                dropped.incrementAndGet();
                return;
            }
        }

        // Enrichment
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("eventId", UUID.randomUUID().toString());
        out.put("ingestedAt", ISO.format(Instant.now()));
        out.put("level", level);
        out.put("endpoint", endpoint);
        out.put("message", message);
        out.put("originalTs", originalTs);
        out.put("worker", Thread.currentThread().getName());

        // Produce to parsed-logs
        try {
            String json = MAPPER.writeValueAsString(out);
            // key by endpoint to enable partition affinity if topic has multiple partitions
            producer.send(new ProducerRecord<>(OUT_TOPIC, endpoint, json));
            forwarded.incrementAndGet();
        } catch (Exception e) {
            sendDlq(producer, "SERIALIZE_ERROR", e.getClass().getSimpleName() + ": " + e.getMessage(), line);
        }
    }

    private static void sendDlq(KafkaProducer<String, String> producer, String reason, String err, String raw) {
        Map<String, Object> dlqPayload = new LinkedHashMap<>();
        dlqPayload.put("reason", reason);
        dlqPayload.put("error", err);
        dlqPayload.put("raw", raw);
        dlqPayload.put("ingestedAt", ISO.format(Instant.now()));
        try {
            String json = MAPPER.writeValueAsString(dlqPayload);
            producer.send(new ProducerRecord<>(DLQ_TOPIC, null, json));
            dlq.incrementAndGet();
        } catch (Exception ex) {
            System.err.println("[DLQ] Failed to serialize DLQ payload: " + ex.getMessage());
            dlq.incrementAndGet();
        }
    }

    private static Properties consumerProps() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "enhanced-log-processor");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return p;
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "10");
        p.put(ProducerConfig.BATCH_SIZE_CONFIG, "32768");
        return p;
    }

    private static String envOr(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }
}
