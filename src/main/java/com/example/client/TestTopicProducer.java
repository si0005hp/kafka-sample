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

    public static void main(String[] args) {
        new TestTopicProducer().run();
    }

    private void run() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "trusty:9092");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new StringSerializer())) {
            
            IntStream.rangeClosed(11, 11).boxed().forEach(i -> {
                try {
                    RecordMetadata meta = producer.send(new ProducerRecord<>("test-topic", 2, "key" + i, "value" + i))
                            .get();
                    System.out.println(String.format("Current offset: %s", meta.offset()));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

}
