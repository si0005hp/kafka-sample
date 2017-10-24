package com.example.client;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestTopicConsumer {
    public static void main(String[] args) {
        new TestTopicConsumer().run();
    }

    private void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "trusty:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-topic-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new StringDeserializer());) {

            consumer.subscribe(Arrays.asList("test-topic"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000L);
                if (records.isEmpty()) {
                    System.out.println("None data.");
                } else {
                    StreamSupport.stream(records.spliterator(), false).forEach(rec -> {
                        System.out.println(String.format("Consumed key: %s, value: %s", rec.key(), rec.value()));
                    });
                }
            }
        }
    }

}
