package com.ideal.kafka.security.ssl.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaSSLProducer {

    String kafkaIpList = "DDP-ETL-J502:9092,DDP-ETL-J503:9092,DDP-ETL-J504:9092,DDP-ETL-J505:9092,DDP-ETL-J602:9092,DDP-ETL-J603:9092,DDP-ETL-J604:9092,DDP-ETL-J605:9092";

    private Properties getSSLProperties(String truststorePWD, String keystorePWD, String keyPWD) {
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", kafkaIpList);
        producerProps.put("security.protocol", "SSL");
//        producerProps.put("ssl.truststore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/client/client.truststore.jks");
        producerProps.put("ssl.truststore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/si-tech/producer/client.truststore.jks");
//        producerProps.put("ssl.truststore.password", truststorePWD);
        producerProps.put("ssl.truststore.password", "qxWci@#;a1pX");
//        producerProps.put("ssl.keystore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/client/client.keystore.jks");
        producerProps.put("ssl.keystore.location", "/home/storm/bin/kafka_2.10-0.10.0.0/ssl_key/si-tech/producer/client.keystore.jks");
//        producerProps.put("ssl.keystore.password", keystorePWD);
        producerProps.put("ssl.keystore.password", "qxWci@#;a1pX");
//        producerProps.put("ssl.key.password", keyPWD);
        producerProps.put("ssl.key.password", "qxWci@#;a1pX");
        producerProps.put("client.id","aclName");
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return producerProps;
    }

    public void startProduce(String truststorePWD, String keystorePWD, String keyPWD) throws Exception {
        KafkaProducer producer = new KafkaProducer(getSSLProperties(truststorePWD, keystorePWD, keyPWD));
        for (int i = 0; i < 100; i++) {

            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("agent2attr", Integer.toString(i), Integer.toString(i)));
            System.out.println("一条消息生成完毕. offset : " + future.get().offset());
            Thread.sleep(500);
        }
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaSSLProducer kafkaSSLProducer = new KafkaSSLProducer();
        kafkaSSLProducer.startProduce(args[0], args[1], args[2]);
    }
}
