package org.hub.alpha;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeEventHandler implements EventHandler {

    private final KafkaProducer<String, String> producer;
    private final String topic;

    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    public WikimediaChangeEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        logger.info("WikiMediaChangeEventHandler has been opened!!");
    }

    @Override
    public void onClosed() {
        producer.close();
        logger.info("WikiMediaChangeEventHandler has been closed!!");
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        String messagePayload = messageEvent.getData();
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, messagePayload);
        producer.send(record);
        logger.info("message has been sent, with following payload >> " + messagePayload);
    }

    @Override
    public void onComment(String s) {
        // throw new UnsupportedOperationException();
    }

    @Override
    public void onError(Throwable throwable) {
        logger.error("error while parsing & reading stream", throwable);
    }

}
