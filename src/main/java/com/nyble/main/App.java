package com.nyble.main;

import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.sql.Date;
import java.util.Properties;

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
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("max.poll.records", 500);
    }


    public static void main(String[] args){

        logger.debug("Start new consumer for group {}", groupId);
        final String query = "INSERT INTO "+ "consumer_actions"+"\n" +
                "(id, system_id, consumer_id, action_id, payload_json, external_system_date, local_system_date) \n" +
                "values (?, ?, ?, ?, ?, ?, ?)";
        final String rmcTopic = "consumer-actions-rmc";
        final String rrpTopic = "consumer-actions-rrp";
        new Thread(()-> consumeTopic(rmcTopic, query)).start();
        new Thread(()-> consumeTopic(rrpTopic, query)).start();

    }

    public static void consumeTopic(String topic, String query){
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Collections.singletonList(topic));
        logger.debug("Consumers connected to topic {}", topic);
        try(Connection conn = DBUtil.getInstance().getConnection();
            PreparedStatement ps = conn.prepareStatement(query)){
            while(true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(1000*10);
                logger.info("Finished poll, records size = {}",records.count());
                try{
                    records.forEach(record ->{
                        try{
                            ConsumerActionsValue cav = ConsumerActionsValue.fromJson(record.value());
                            ps.setInt(1, cav.getId());
                            ps.setInt(2, cav.getSystemId());
                            ps.setInt(3, cav.getConsumerId());
                            ps.setInt(4, cav.getActionId());
                            PGobject jsonb = new PGobject();
                            jsonb.setType("jsonb");
                            jsonb.setValue(cav.getPayloadJson().getRaw());
                            ps.setObject(5, jsonb);
                            ps.setDate(6, new Date(Long.parseLong(cav.getExternalSystemDate())));
                            ps.setDate(7, new Date(Long.parseLong(cav.getLocalSystemDate())));
                            ps.executeUpdate();
                            logger.debug("Inserted action to table");
                        } catch (SQLException e) {
                            logger.error(e.getMessage(), e);
                        }

                    });
                }catch(Exception e){
                    logger.error(e.getMessage(), e);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
