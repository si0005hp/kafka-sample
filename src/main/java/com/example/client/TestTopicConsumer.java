package com.example.client;

import static com.example.client.TestTopicProducer.TOPIC;

import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class TestTopicConsumer {
    public static void main(String[] args) {
        new TestTopicConsumer().run();
    }

    private void run() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-topic-consumer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties, new StringDeserializer(),
                new StringDeserializer());) {

            consumer.subscribe(Arrays.asList(TOPIC));

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
    
    @SuppressWarnings("unused")
    private void seekOffset(KafkaConsumer<String, String> consumer, TopicPartition partition, long offset) {
        consumer.poll(1000L); // ensure consumer is subscribing
        consumer.seek(partition, offset);
    }

}
