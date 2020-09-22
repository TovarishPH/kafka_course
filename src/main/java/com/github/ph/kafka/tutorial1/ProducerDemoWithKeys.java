package com.github.ph.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

        String bootstrapServer = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "Hello World " + Integer.toString(i);
            String key = "id_" + Integer.toString(i); //uma key sempre irá para a mesma partition

            //create producer record
            final ProducerRecord<String, String> record =
                            new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);
            /*resultado com três partitions:
                id_0 partition 1
                id_1 partition 0
                id_2 partition 2
                id_3 partition 0
                id_4 partition 2
                id_5 partition 2
                id_6 partition 0
                id_7 partition 2
                id_8 partition 1
                id_9 partition 2
             */

            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Recieved new metadata. " + "\n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing" + e);
                    }
                }
            }).get(); //torna síncorono, apenas para teste. Não fazer em produção!
        }

        // flush data
        producer.flush();

        //flush and close
        producer.close();
    }
}
