package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");
        String topic = "demo_kafka_java";

        // setup properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // define the record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, "hello world this is okza!");

        // send the records to topic
        producer.send(producerRecord);

        // tell the Producer to send all data and block until done -- synchronous process
        producer.flush();

        // close the producer
        producer.close();

        log.info("Successfully send data to Kafka topic!");

    }
}
