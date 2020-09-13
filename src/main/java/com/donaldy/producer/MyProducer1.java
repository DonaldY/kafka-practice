package com.donaldy.producer;

import com.sun.xml.internal.ws.api.message.Header;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author donald
 * @date 2020/09/12
 */
public class MyProducer1 {

    public static void main(String[] args) throws InterruptedException,
            ExecutionException, TimeoutException {

        Map<String, Object> configs = new HashMap<>();
        // 设置连接Kafka的初始连接用到的服务器地址
        // 如果是集群,则可以通过此初始连接发现集群中的其他broker
        configs.put("bootstrap.servers", "node1:9092");
        // 设置key的序列化器
        configs.put("key.serializer",
                "org.apache.kafka.common.serialization.IntegerSerializer");
        // 设置value的序列化器
        configs.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        configs.put("acks", "1");


        KafkaProducer<Integer, String> producer = new KafkaProducer<>(configs);

        // 用于设置用户自定义的消息头字段
        // List<RecordHeader> headers = Collections.singletonList(new RecordHeader("biz.name", "producer demo".getBytes()));

        // 主题名称
        // 分区编号,现在只有一个分区,所以是0
        // 数字作为key
        // 字符串作为value
        // 用于封装Producer的消息
        ProducerRecord<Integer, String> record = new ProducerRecord<>(
                "topic_1",
                0,
                0,
                "donald message 0"
        );

        // 发送消息,同步等待消息的确认
        final Future<RecordMetadata> future = producer.send(record);

        final RecordMetadata metadata = future.get();

        System.out.println("消息的主题 ： " + metadata.topic());
        System.out.println("消息的分区号 : " + metadata.partition());
        System.out.println("消息的偏移量 ： " + metadata.offset());

        // 关闭生产者
        producer.close();
    }
}
