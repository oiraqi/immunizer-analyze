package org.immunizer.microservices.analyzer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;

import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.time.Duration;
import java.util.regex.Pattern;

public class FeatureRecordConsumer {

    private Consumer<String, FeatureRecord> consumer;
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "Analyzer";
    private static final String TOPIC_PATTERN = "FeatureRecords/.+";

    public FeatureRecordConsumer () {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", GROUP_ID);
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.immunizer.microservices.analyzer.FeatureRecordDeserializer");

        consumer = new KafkaConsumer<String, FeatureRecord>(props);
        consumer.subscribe(Pattern.compile(TOPIC_PATTERN));
    }

    public ConsumerRecords<String, FeatureRecord> poll (Duration timeout) {
        return consumer.poll(timeout);
    }

    public void close () {
        consumer.close();
    }
}