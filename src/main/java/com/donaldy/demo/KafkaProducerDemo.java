package com.donaldy.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

public class KafkaProducerDemo {

    private KafkaProducer<String, String> producer;

    public KafkaProducerDemo() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.226.150:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<String, String>(props);
    }

    public void write(List<ConsumerRecord> dataList) {

        if (null == dataList || dataList.isEmpty()) {

            return;
        }

        for (ConsumerRecord record : dataList) {

            System.out.println("write to topic: " + record.topic());

            producer.send(new ProducerRecord<String, String>(record.topic(), record.value().toString()));
        }

        producer.close();
    }
}
