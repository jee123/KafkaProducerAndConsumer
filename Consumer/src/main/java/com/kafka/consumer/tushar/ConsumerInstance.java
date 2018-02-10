
/*
 * Worker thread that defines logic to handle processing of message dispatched from main method.
 */

package com.kafka.consumer.tushar;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.common.errors.WakeupException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerInstance implements Runnable{
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final List<String> topics;
    //map that stores the record attribute name as key and timestamp, record value and offset as value.
    private ConcurrentHashMap<String , Object> map = new ConcurrentHashMap<>();
    private int port;
    private String host;
    private AtomicInteger count;

    public ConsumerInstance(String groupId,
                            List<String> topics,
                            String host,
                            int port,
                            AtomicInteger count) {
        this.topics = topics;
        this.host = host;
        this.port = port;
        this.count = count;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.consumer = new KafkaConsumer<>(props);
    }



    /**
     * logic for each thread that includes subscribing to topic, polling records and populating the map.
     */
    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
            while (true) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
                createPartitionIdKey(records);
                System.out.println("Map contents are ==> " + map);
            }
        } catch (WakeupException e) {
            System.out.println("Finished polling for records, exit now.");
        } finally {
            consumer.close();
        }
    }


    /**
     * This method gets records belonging to same partition id and pushed them to the map.
     *
     * @param records : polled records from kafka topic.
     */

    private void createPartitionIdKey(ConsumerRecords<byte[], byte[]> records)
            throws RetriableException{
        for (TopicPartition topicPartition : records.partitions()) {
            List<ConsumerRecord<byte[], byte[]>> partitionRecords = records.records(topicPartition);
            try {
                populateMap(partitionRecords);
            }catch (KafkaException e){
                e.printStackTrace();
                throw new RetriableException("Unable to save records in hashmap.");

            }
        }
    }

    /**
     * populate hashmap using record attribiute names as key.
     *
     * @param partitionRecords : record/records that belong to a particular partition id.
     */

    void populateMap(List<ConsumerRecord<byte[], byte[]>> partitionRecords) {
        for (ConsumerRecord<byte[], byte[]> record : partitionRecords) {
            map.put("partition id", record.partition());
            map.put("timestamp value", record.timestamp());
            map.put("offset", record.offset());
            map.put("value", record.value());
        }
    }


    /**
     * wake up consumer and exit polling loop.
     */
    public void shutdown() {
        System.out.println("DEBUG -> Inside shutdown()");
        consumer.wakeup();
    }


}
