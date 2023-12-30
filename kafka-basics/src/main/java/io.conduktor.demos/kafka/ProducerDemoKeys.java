package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    // https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/
    // using Sticky Partitioner, can also use RoundRobinPartitioner
    private static final String partitioner = DefaultPartitioner.class.getName();

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

        String topic = "demo_kafka_java";
        log.info("Using " + partitioner + " partitioner");


        for (int batch=0; batch<2; batch++){
            log.info("Batch: " + batch);

            for (int idx=0; idx<10; idx++){
                String key = "id_" + idx;
                String value = "just a value with key defined " + idx;

                // define the record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send the records to topic
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e == null){
                            log.info("Key: " + key + ", " +
                                    "Partition: " + recordMetadata.partition());
                        } else {
                            log.error("Exception occurred", e);
                        }
                    }
                });
            }
        }

        // tell the Producer to send all data and block until done -- synchronous process
        producer.flush();

        // close the producer
        producer.close();
        log.info("Successfully send data to Kafka topic!");

    }
}
