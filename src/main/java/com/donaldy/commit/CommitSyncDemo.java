package com.donaldy.commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * @author donald
 * @date 3/2/20
 */
public class CommitSyncDemo {

    private KafkaConsumer<String, String> consumer;

    public CommitSyncDemo() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "");
        props.put("group.id", "test-group");
        // Commit Sync
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Collections.singleton(""));

        final int minBatchSize = 500;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();

        while (true) {

            ConsumerRecords<String, String> records = consumer.poll(1000);

            for (ConsumerRecord<String, String> record : records) {

                buffer.add(record);
            }

            // 满500就更新
            if (buffer.size() >= minBatchSize) {

                // transaction
                consumer.commitSync();
                buffer.clear();
            }
        }
    }
}
