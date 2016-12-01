package com.ideal.kafka.security.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaSecurityConsumer {

    public static void main(String[] args) {
        try {
            new KafkaSecurityConsumer().testConsumer();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //加载kafka认证文件
    static{
        System.setProperty("java.security.auth.login.config", "/root/kafka_client_plaintext_jaas.conf");
    }

    public void testConsumer() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.29.145:10092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList("my_topic_1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            System.out.println("records.count:" + records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s, partition = %d %n",
                        record.offset(),
                        record.key(),
                        record.value(),
                        record.partition());
            }
        }
    }
}
