package com.donaldy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
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


        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(configs);

        // 主题名称
        // 分区编号,现在只有一个分区,所以是0
        // 数字作为key
        // 字符串作为value
        // 用于封装Producer的消息
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(
                "topic_1",
                 0,
                 0,
                 "donald message 0"
        );

         // 发送消息,同步等待消息的确认
         producer.send(record).get(3_000, TimeUnit.MILLISECONDS);

        // 关闭生产者
        producer.close();
    }
}
