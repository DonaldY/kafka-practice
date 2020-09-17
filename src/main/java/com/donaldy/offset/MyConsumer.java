package com.donaldy.offset;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author donald
 * @date 2020/09/17
 */
public class MyConsumer {
    public static void main(String[] args) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // group.id很重要
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "mygrp1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

        consumer.subscribe(Arrays.asList("tp_demo_01"));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(1_000);

            records.forEach(record -> System.out.println(record));
        }

    }
}