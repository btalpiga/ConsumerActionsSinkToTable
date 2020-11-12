package com.nyble.main;

import com.nyble.exceptions.RuntimeSqlException;
import com.nyble.facades.kafkaConsumer.RecordProcessor;
import com.nyble.topics.TopicObjectsFactory;
import com.nyble.topics.consumerActions.ConsumerActionsValue;
import com.nyble.util.DBUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class RecordProcessorImpl implements RecordProcessor<String, String> {

    final static private Logger logger = LoggerFactory.getLogger(RecordProcessorImpl.class);
    final String query = "INSERT INTO "+ "consumer_actions"+"\n" +
            "(id, system_id, consumer_id, action_id, payload_json, external_system_date, local_system_date) \n" +
            "values (?, ?, ?, ?, ?, ?, ?)";


    @Override
    public boolean process(ConsumerRecord<String, String> record) {
        throw new UnsupportedOperationException("Processing single kafka consumer record in ConsumerActionsSinktToTable not supported");
    }

    @Override
    public boolean processBatch(ConsumerRecords<String, String> records) {
        try(Connection conn = DBUtil.getInstance().getConnection("datawarehouse");
            PreparedStatement ps = conn.prepareStatement(query)){

            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(false);
            for(ConsumerRecord<String, String> rec : records){
                ConsumerActionsValue cav = (ConsumerActionsValue) TopicObjectsFactory.fromJson(rec.value(),
                        ConsumerActionsValue.class);
                storeActionToDB(cav, ps);
            }

            conn.commit();
            conn.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            throw new RuntimeSqlException(e.getMessage(), e);
        }
        return true;
    }

    private void storeActionToDB(ConsumerActionsValue cav, PreparedStatement ps){
        try{
            ps.setInt(1, Integer.parseInt(cav.getId()));
            ps.setInt(2, Integer.parseInt(cav.getSystemId()));
            ps.setInt(3, Integer.parseInt(cav.getConsumerId()));
            ps.setInt(4, Integer.parseInt(cav.getActionId()));
            PGobject jsonb = new PGobject();
            jsonb.setType("jsonb");
            jsonb.setValue(cav.getPayloadJson().getRaw());
            ps.setObject(5, jsonb);
            ps.setDate(6, new Date(Long.parseLong(cav.getExternalSystemDate())));
            ps.setDate(7, new Date(Long.parseLong(cav.getLocalSystemDate())));
            ps.executeUpdate();
            logger.debug("Inserted action to table");
        } catch (SQLException e) {
            throw new RuntimeSqlException(e.getMessage(), e);
        }

    }
}
