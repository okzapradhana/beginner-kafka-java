package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getName());

        Properties properties = new Properties();
        // connect to the cluster
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");

        // set serializer for key and value
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set this config for safe producer (for Kafka <= 2.8.0)
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // same as -1
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.toString(120000));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.toString(5));

        // set high throughput configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(20)); // 20ms time to wait before sending batch to topic
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(3 * 1024)); // 36KB size
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // balance in CPU usage and compression ratio


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String eventURI = "https://stream.wikimedia.org/v2/stream/recentchange";
        String topic = "wikimedia.recentchanges";

        BackgroundEventHandler eventHandler = new WikimediaChangesHandler(producer, topic);
        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventHandler, new EventSource.Builder(URI.create(eventURI)));
        BackgroundEventSource eventSource = builder.build();

        // start the producer in another thread
        eventSource.start();

        // we produce the data for 10 minutes and block the program until then -> sleep the thread
        TimeUnit.MINUTES.sleep(10);

    }
}
