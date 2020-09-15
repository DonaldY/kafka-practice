package com.donaldy.serializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * @author donald
 * @date 2020/09/15
 */
public class MyProducer {

    public static void main(String[] args) {

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");

        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
// 设置自定义的序列化类


        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                UserSerializer.class);


        KafkaProducer<String, User> producer = new KafkaProducer<>(configs);

        User user = new User();
        user.setUserId(1001);
        user.setUsername("张三");


        ProducerRecord<String, User> record = new ProducerRecord<>(
                 "tp_user_01",
                 0,
                 user.getUsername(),
                 user
        );


        producer.send(record, (metadata, exception) -> {

            if (exception == null) {
                System.out.println("消息发送成功:"
                        + metadata.topic() + "\t"
                        + metadata.partition() + "\t"
                        + metadata.offset());

            } else {

                System.out.println("消息发送异常");

            }

        });

        // 关闭生产者
        producer.close();


    }
}
