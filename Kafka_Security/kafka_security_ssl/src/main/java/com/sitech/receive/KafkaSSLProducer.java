package com.sitech.receive;

//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.Future;


public class KafkaSSLProducer {
//    private Logger logger = Logger.getLogger("PRE");
    static String kafkaIpList = "10.5.30.90:9092,10.5.30.91:9092,10.5.30.92:9092,10.5.30.93:9092,10.5.30.94:9092,10.5.30.95:9092,10.5.30.96:9092,10.5.30.97:9092";

    private Properties getSSLProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kafkaIpList);
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location","/data/cdpi/zxc/kafka/si-tech/producer/client.truststore.jks");//路径
        properties.put("ssl.truststore.password", "qxWci@#;a1pX");
        properties.put("ssl.keystore.location", "/data/cdpi/zxc/kafka/si-tech/producer/client.keystore.jks");
        properties.put("ssl.keystore.password","qxWci@#;a1pX");
        properties.put("ssl.key.password", "qxWci@#;a1pX");
        properties.put("client.id","agent2attr");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks","all");
        properties.put("retries",0);
        properties.put("batch.size",16384);
        properties.put("linger.ms",1);
        properties.put("buffer.memory",1048576);
        return properties;
    }

    public void startProduce() throws Exception {
        System.out.println("开始连接");
        KafkaProducer producer = new KafkaProducer(getSSLProperties());
        System.out.println("连接成功");
        for (int i = 0; i < 100; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>("agent2attr", Integer.toString(1), Integer.toString(1)));
            System.out.println("一条消息生成完毕. offset : " + future.get().offset());
            Thread.sleep(500);
        }
        producer.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaSSLProducer kafkaSSLProducer = new KafkaSSLProducer();
        kafkaSSLProducer.startProduce();
    }
}