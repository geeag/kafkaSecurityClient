package com.ideal.kafka.security.product;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSecurityProducer {

    //加载kafka认证文件
    static{
        System.setProperty("java.security.auth.login.config", "/root/kafka_client_plaintext_jaas.conf");
    }


    public static void main(String[] args) {
        try {
            new KafkaSecurityProducer().testProduct();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testProduct() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.29.145:10092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, String>("my_topic_1", Integer.toString(i), Integer.toString(i)));
            System.out.println("一条消息生成完毕.");
            Thread.sleep(1000);
        }
        producer.flush();
        producer.close();
    }
}
