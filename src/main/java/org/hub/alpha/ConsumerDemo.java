package org.hub.alpha;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        logger.info("I am Kafka consumer");

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my_first_group";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("exiting process has been detected, calling consumer.wakeup() ..");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
//            consumer.subscribe(Arrays.asList("java_demo"));
            consumer.subscribe(List.of("td"));

            while (true) {
//                logger.info("polling ..");

                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("Partition: " + record.partition() + " || " + "Offset: " + record.offset() + " || " + "Key: " + record.key() + " || " + "Value: " + record.value());
                }
            }
        } catch (WakeupException e) {
            logger.info("wakeup exception!!");
        } catch (Exception e) {
            logger.error("unexpected exception has occurred", e);
        } finally {
            consumer.close();
            logger.info("the consumer has been shutdown gracefully!!");
        }

    }

}
