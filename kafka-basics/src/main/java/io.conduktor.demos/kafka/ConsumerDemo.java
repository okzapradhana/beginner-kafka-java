package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");

        String groupId = "java-app-group";
        String topic = "demo_kafka_java";

        // consumer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:19092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create the Kafka Producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe to a topic
        consumer.subscribe(List.of(topic));
        while (true){
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Consumed record metadata: " + "\n" +
                        "Key: " + record.key() + "," +
                        "Value: " + record.value() + "\n" +
                        "Topic: " + record.topic() + "," +
                        "Partition: " + record.partition() + "," +
                        "Offset: " + record.offset());
            }
        }
    }
}
