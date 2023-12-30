package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesHandler implements BackgroundEventHandler {
    KafkaProducer<String, String> producer;
    String topic;
    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesHandler.class.getSimpleName());

    WikimediaChangesHandler(KafkaProducer<String, String> producer, String topic){
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {
        // no implementation here
    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<String, String>(this.topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s) throws Exception {
        // no implementation here
    }

    @Override
    public void onError(Throwable throwable) {
        log.error(throwable.getMessage());
        producer.close();
    }
}
