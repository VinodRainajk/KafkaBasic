package com.wellsfargo.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemo {

    private static  final Logger logger = LoggerFactory.getLogger(producerDemo.class);
    public static void main(String[] args) {
    logger.info("Hello World");

        //Create Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //Create Data to be sent
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_java", "Hello From Intellij");
        //Send Data
        producer.send(producerRecord);
        //flush Producer
        //producer.flush();
        //Close Producer
        producer.close();
    }

}
