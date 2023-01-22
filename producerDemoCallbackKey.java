package com.wellsfargo.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producerDemoCallbackKey {

    private static  final Logger logger = LoggerFactory.getLogger(producerDemoCallbackKey.class);
    public static void main(String[] args) {
    logger.info("Hello World");

        //Create Producer
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //Create Data to be sent
        String topicname =  "demo_java";
        String datavalue = "Testin ";
        String keyvalue = "i_ ";

       for(int idx = 0; idx<9;idx++)
       {

           ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicname,keyvalue+idx, datavalue+idx);
           //Send Data
           producer.send(producerRecord, new Callback() {
               @Override
               public void onCompletion(RecordMetadata metadata, Exception exception) {
                   logger.info("topic name "+metadata.topic()+ " \n"+
                           "Partition " +metadata.partition() + " \n"+
                           "Key " + producerRecord.key()+ " \n"+
                           "offset " +metadata.offset()
                   );
               }
           });
       }

        //flush Producer
        //producer.flush();
        //Close Producer
        producer.close();
    }

}
