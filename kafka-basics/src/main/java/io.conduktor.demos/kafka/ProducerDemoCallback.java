package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class.getSimpleName());
    // https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/
    // using Sticky Partitioner, can also use RoundRobinPartitioner
    private static String partitioner = DefaultPartitioner.class.getName();

    public static void main(String[] args) {
        log.info("Hello world!");

        // setup properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("partitioner.class", partitioner);

        // create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        log.info("Using " + partitioner + " partitioner");
        for (int i=0; i<10;i++){
            log.info("Current loop: " + i);
            // define the record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_kafka_java", "hello world this record utilizes callback func! " + i);

            // send the records to topic
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null){
                        log.info("Received record metadata + \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n");
                    } else {
                        log.error("Exception occurred", e);
                    }
                }
            });
        }

        // tell the Producer to send all data and block until done -- synchronous process
        producer.flush();

        // close the producer
        producer.close();
        log.info("Successfully send data to Kafka topic!");

    }
}
