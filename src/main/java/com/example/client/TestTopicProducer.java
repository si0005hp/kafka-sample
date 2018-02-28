package com.example.client;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class TestTopicProducer {
    
    public static final String TOPIC = "test-topic";

    public static void main(String[] args) {
        new TestTopicProducer().run();
    }

    private void run() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, String.valueOf(Long.MAX_VALUE));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new StringSerializer())) {
            
            IntStream.rangeClosed(9, 9).boxed().forEach(i -> {
                try {
                    RecordMetadata meta = producer.send(new ProducerRecord<>(TOPIC, 0, "key" + i, "value" + i))
                            .get();
                    System.out.println(String.format("Current offset: %s", meta.offset()));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

}
