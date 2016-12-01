package com.ideal.kafka.security.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

public class KafkaSecurityKerberosConsumer {

    public static void main(String[] args) {
        try {
            new KafkaSecurityKerberosConsumer().testConsumer(args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //加载kafka认证文件
    static{
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/home/chenpeng/bin/kerberos_conf/client/client_jaas.conf");
    }

    public void testConsumer(String ips) throws Exception {
        Properties props = new Properties();
//        props.put("bootstrap.servers", "ddp-qx-p0204:2181,ddp-qx-p0205:2181,ddp-qx-p0501:2181");
        props.put("bootstrap.servers", ips);
//        props.put("bootstrap.servers", "ddp-qx-p0204:9092,ddp-qx-p0205:9092,ddp-qx-p0501:9092,ddp-qx-p0502:9092,ddp-qx-p0505:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "chenpeng");
        String topic = "kdc_topic";


        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            System.out.println("records.count:" + records.count());
            for (ConsumerRecord<String, String> record :  records) {
                System.out.printf("offset = %d, key = %s, value = %s, partition = %d %n",
                        record.offset(),
                        record.key(),
                        record.value(),
                        record.partition());
            }
        }
    }
}
