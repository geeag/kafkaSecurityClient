package com.ideal.kafka.security.ssl.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class KafkaSSLConsumer {

    String kafkaIpList = "DDP-ETL-J502:9092,DDP-ETL-J503:9092,DDP-ETL-J504:9092,DDP-ETL-J505:9092,DDP-ETL-J602:9092,DDP-ETL-J603:9092,DDP-ETL-J604:9092,DDP-ETL-J605:9092";

    private Properties getSSLProperties(String truststorePWD, String keystorePWD, String keyPWD) {
        Properties producerProps = new Properties();
        producerProps.put("group.id", "ssl_groupId");
        producerProps.put("bootstrap.servers", kafkaIpList);
        producerProps.put("security.protocol", "SSL");
        producerProps.put("ssl.truststore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/si-tech/consumer/client.truststore.jks");
//        producerProps.put("ssl.truststore.password", truststorePWD);
        producerProps.put("ssl.truststore.password", "ci#iOEs,suwz3");
        producerProps.put("ssl.keystore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/si-tech/consumer/client.keystore.jks");
//        producerProps.put("ssl.keystore.password", keystorePWD);
        producerProps.put("ssl.keystore.password", "ci#iOEs,suwz3");
//        producerProps.put("ssl.key.password", keyPWD);
        producerProps.put("ssl.key.password", "ci#iOEs,suwz3");
        producerProps.put("client.id","aclName");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        producerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return producerProps;
    }

    public void startConsume(String truststorePWD, String keystorePWD, String keyPWD) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(getSSLProperties(truststorePWD, keystorePWD, keyPWD));
        consumer.subscribe(Collections.singletonList("seaHigh"));
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


    public static void main(String[] args) {
        KafkaSSLConsumer kafkaSSLConsumer = new KafkaSSLConsumer();
        kafkaSSLConsumer.startConsume(args[0], args[1], args[2]);
    }
}
