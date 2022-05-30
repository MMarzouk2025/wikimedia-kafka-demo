package org.hub.alpha;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "java_demo";

//        for (int i = 1; i <= 10; i++) {
//            ProducerRecord<String, String> record = new ProducerRecord<>("java_demo", "message-" + i);
//            producer.send(record, (metadata, exception) -> {
//                if (exception == null) {
//                    logger.info(
//                            "message: " + record.value() + " => sent successfully, topic: " + metadata.topic()
//                                    + " - partition: " + metadata.partition()
//                                    + " - offset: " + metadata.offset()
//                                    + " - time: " + metadata.timestamp()
//                    );
//                } else {
//                    logger.error("error while producing", exception);
//                }
//            });
//        }

        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, "id_8", "message-1");
        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, "id_9", "message-2");
        ProducerRecord<String, String> record3 = new ProducerRecord<>(topic, "id_8", "message-3");
        ProducerRecord<String, String> record4 = new ProducerRecord<>(topic, "id_10", "message-4");
        ProducerRecord<String, String> record5 = new ProducerRecord<>(topic, "id_12", "message-5");
        ProducerRecord<String, String> record6 = new ProducerRecord<>(topic, "id_11", "message-6");
        ProducerRecord<String, String> record7 = new ProducerRecord<>(topic, "id_11", "message-7");
        ProducerRecord<String, String> record8 = new ProducerRecord<>(topic, "id_10", "message-8");

        producer.send(record1, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record1.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record1.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record2, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record2.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record2.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record3, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record3.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record3.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record4, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record4.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record4.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record5, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record5.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record5.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record6, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record6.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record6.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record7, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record7.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record7.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });
        producer.send(record8, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "message: " + record8.value() + " => sent successfully, topic: " + metadata.topic()
                                + " - key: " + record8.key()
                                + " - partition: " + metadata.partition()
                                + " - offset: " + metadata.offset()
                                + " - time: " + metadata.timestamp()
                );
            } else {
                logger.error("error while producing", exception);
            }
        });

        producer.flush();

        producer.close();
    }

}
