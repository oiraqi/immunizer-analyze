package org.immunizer.microservices.analyzer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;
import java.util.regex.Pattern;
import java.util.Vector;
import java.util.List;

import scala.Tuple2;

public class FeatureRecordConsumer {

    private Consumer<String, FeatureRecord> consumer;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "Analyzer";
    private static final String TOPIC_PATTERN = "FRC/.+";

    public FeatureRecordConsumer () {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", GROUP_ID);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.immunizer.microservices.analyzer.FeatureRecordDeserializer");

        consumer = new KafkaConsumer<String, FeatureRecord>(props);
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));
    }

    public List<Tuple2<String, IdentifiableFeatureRecord>> poll (int timeout, int minBatchSize) {
        ConsumerRecords<String, FeatureRecord> records = consumer.poll(Duration.ofSeconds(timeout));
        Vector<Tuple2<String, IdentifiableFeatureRecord>> featureRecords = 
            new Vector<Tuple2<String, IdentifiableFeatureRecord>>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, FeatureRecord>> partitionRecords = records.records(partition);
            if(partitionRecords.size() < minBatchSize)
                continue;            
            
            for (ConsumerRecord<String, FeatureRecord> record : partitionRecords) {
                featureRecords.add(new Tuple2<String, IdentifiableFeatureRecord>(
                    record.value().getSwid() + '/' + record.value().getCallStackId(),
                    new IdentifiableFeatureRecord(record.offset(), record.value())));
            }
            long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
        }
        return featureRecords;
    }

    public void close () {
        consumer.close();
    }
}