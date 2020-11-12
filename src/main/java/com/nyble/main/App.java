package com.nyble.main;

import com.nyble.facades.kafkaConsumer.KafkaConsumerFacade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SpringBootApplication(scanBasePackages = {"com.nyble.rest"})
public class App {

    final static String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "10.100.1.17:9093";
    final static String groupId = "consumer-actions-to-db";
    static Logger logger = LoggerFactory.getLogger(App.class);
    static Properties consumerProps = new Properties();
    static {
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
    }


    public static void main(String[] args){
        SpringApplication.run(App.class, args);

        logger.debug("Start new consumer for group {}", groupId);
        final String rmcTopic = "consumer-actions-rmc";
        final String rrpTopic = "consumer-actions-rrp";

        KafkaConsumerFacade<String, String> rmcActionsConsumer = new KafkaConsumerFacade<>(consumerProps,
                1, KafkaConsumerFacade.PROCESSING_TYPE_BATCH);
        rmcActionsConsumer.subscribe(Collections.singletonList(rmcTopic));
        rmcActionsConsumer.startPolling(Duration.ofSeconds(10), RecordProcessorImpl.class);

        KafkaConsumerFacade<String, String> rrpActionsConsumer = new KafkaConsumerFacade<>(consumerProps,
                1, KafkaConsumerFacade.PROCESSING_TYPE_BATCH);
        rrpActionsConsumer.subscribe(Collections.singletonList(rrpTopic));
        rrpActionsConsumer.startPolling(Duration.ofSeconds(10), RecordProcessorImpl.class);
    }
}
