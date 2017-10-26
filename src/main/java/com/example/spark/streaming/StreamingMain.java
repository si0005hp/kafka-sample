package com.example.spark.streaming;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.spark_project.guava.collect.ImmutableMap;
import org.spark_project.guava.collect.Lists;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.google.common.collect.ImmutableSet;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StreamingMain {
    public static void main(String[] args) {
        try {
            new StreamingMain().process(args);
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed.", e);
        }
    }

    private void process(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[4]").setAppName("app");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

        generateStream(ssc, sc);

        ssc.start();
        ssc.awaitTermination();
    }

    private void generateStream(JavaStreamingContext ssc, JavaSparkContext sc) {
        KafkaSetting<String, String> kafkaSetting = createKafkaSetting(String.class, String.class);
        
        JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(ssc,
                kafkaSetting.getKeyClass(),
                kafkaSetting.getValueClass(),
                kafkaSetting.getKeyDecoderClass(),
                kafkaSetting.getValueDecoderClass(), 
                kafkaSetting.getProperties(),
                kafkaSetting.getTopics()
                );

        
        log.info("Streaming process started...");
        stream.foreachRDD(rdd -> {
            rdd.foreachPartition(partition -> {
                Lists.newArrayList(partition).stream().forEach(msg -> {
                    log.info("Fetched message -> key: {}, value: {}", msg._1, msg._2);
                });
            });
        });
    }

    private <K, V> KafkaSetting<K, V> createKafkaSetting(Class<K> keyClass, Class<V> valueClass) {
        Map<String, String> properties = ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "trusty:9092", 
                ConsumerConfig.GROUP_ID_CONFIG,"test-topic-consumer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Set<String> topics = ImmutableSet.of("test-topic");

        @SuppressWarnings("unchecked")
        KafkaSetting<K, V> setting = KafkaSetting.of(properties,
                keyClass, valueClass,
                Class.class.cast(StringDecoder.class),
                Class.class.cast(StringDecoder.class),
                topics);
        
        return setting;
    }

    @Value(staticConstructor = "of")
    static class KafkaSetting<K, V> {
        Map<String, String> properties;
        Class<K> keyClass;
        Class<V> valueClass;
        Class<Decoder<K>> keyDecoderClass;
        Class<Decoder<V>> valueDecoderClass;
        Set<String> topics;
    }
}
