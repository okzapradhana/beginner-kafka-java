package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

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

        // default: partition.assignment.strategy = [class org.apache.kafka.clients.consumer.RangeAssignor, class org.apache.kafka.clients.consumer.CooperativeStickyAssignor]
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create the Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get a reference to main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run(){
                log.info("Detected a shutdown. I'm going to call consumer.wakeup() from another Thread");
                consumer.wakeup();

                try {
                    // join the main thread to allow the code execution in main thread
                    mainThread.join();
                } catch (InterruptedException e ){
                    log.error(e.getMessage());
                }
            }
        });

        try {
            // subscribe to a topic
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Consumed record metadata: " + "\n" +
                            "Key: " + record.key() + ", " +
                            "Value: " + record.value() + "\n" +
                            "Topic: " + record.topic() + ", " +
                            "Partition: " + record.partition() + "," +
                            "Offset: " + record.offset());
                }
            }
        } catch (WakeupException e){
            log.info("The consumer is starting to shutdown");
        } catch (Exception e){
            log.error("Something happened in our consumer", e);
        } finally {
            consumer.close(); // close the consumer and also committing the offsets.
            log.info("The consumer is now gracefully shutdown");
        }
    }
}
