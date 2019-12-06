package com.donaldy.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class KafkaConsumerDemo {

    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerDemo(String topicName) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.94.22:6667,192.168.94.25:6667,192.168.94.26:6667");
        props.put("group.id", "test_yyf_12345678990");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("max.poll.records", 1000);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Collections.singleton(topicName));
    }

    public List<ConsumerRecord> read(int size) throws Exception {

        if (size <= 0) {

            return Collections.emptyList();
        }

        List<ConsumerRecord> dataList = new ArrayList<ConsumerRecord>(size);

        try {

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(3000);

                if (records.isEmpty()) {

                    continue;
                }

                for (ConsumerRecord<String, String> consumerRecord : records) {

                    dataList.add(consumerRecord);

                    System.out.println(consumerRecord.value());

                    if (dataList.size() >= size) {

                        return dataList;
                    }
                }
            }

        } catch (Exception e) {

            e.printStackTrace();
        }

        return dataList;
    }
}
