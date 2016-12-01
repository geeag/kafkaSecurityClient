package com.ideal.kafka.security.product;

import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaSecurityKerberosProducer {

    //加载kafka认证文件
    static{
        System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/home/chenpeng/bin/kerberos_conf/client/client_jaas.conf");
    }


    public static void main(String[] args) {
        try {
            new KafkaSecurityKerberosProducer().testProduct(args[0],Integer.parseInt(args[1]));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testProduct(String ips,int total) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", ips);
//        props.put("bootstrap.servers", "ddp-qx-p0204:9092,ddp-qx-p0205:9092,ddp-qx-p0501:9092,ddp-qx-p0502:9092,ddp-qx-p0505:9092");
        props.put("metadata.broker.list", ips);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "chenpeng");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        String topic = "kdc_topic";
        new kafka.javaapi.producer.Producer<String, String>(new ProducerConfig(props));
        for(int i = 0; i < total; i++){
            producer.send(new ProducerRecord<String, String>(topic,"ddp-qx-p0204:9092,ddp-qx-p0205:9092,ddp-qx-p0501:9092,ddp-qx-p0502:9092,ddp-qx-p0505:9092 ------message" + i));
            System.out.println("message" + i + "消息生成完毕.");
            Thread.sleep(200);
        }
        producer.flush();
        producer.close();
        List l = new ArrayList();
    }
}
