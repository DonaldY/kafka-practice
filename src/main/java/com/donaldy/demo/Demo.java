package com.donaldy.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class Demo {

    /**
     * 1. 从 kafka 中读取数据
     * 2. 写入 kafka
     */
    public static void main(String[] args) throws Exception {

        System.out.println("==== start kafka consumer =====");

        KafkaConsumerDemo kafkaConsumerDemo = new KafkaConsumerDemo("bfd_po_bbs_reply");

        List<ConsumerRecord> data = kafkaConsumerDemo.read(10);

        System.out.println("==== stop kafka consumer ====");

        Thread.sleep(10000);

        System.out.println("==== start kafka producer ====");

        KafkaProducerDemo kafkaProducerDemo = new KafkaProducerDemo();

        System.out.println("==== stop kafka producer ====");

        kafkaProducerDemo.write(data);

        Thread.sleep(10000);

    }
}
