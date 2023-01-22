package com.wellsfargo.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class consumerDemo {

    private static  final Logger logger = LoggerFactory.getLogger(consumerDemo.class);
    public static void main(String[] args) {
    logger.info("Hello World");

        //Create Producer
        Properties properties = new Properties();
        String groupid = "consumer_group";
        String topic = "demo_java";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG ,groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ,"earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singleton(topic));

        while(true)
        {
            logger.info("polling");
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> rcd : records)
            {
                logger.info("Key " +rcd.key()+ " Value "+rcd.value());
                logger.info("partition " +rcd.partition()+ " Value "+rcd.offset());
            }


        }
    }

}
