package org.immunizer.microservices.analyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;

public class Analyzer {

    private static final String SPARK_MASTER_URL = "spark://spark-master:7077";
    private static final String APP_NAME = "Analyzer";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "Analyzer";
    private static final String TOPIC_PATTERN = "FRC/.+";
    private static final int BATCH_DURATION = 60;

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(APP_NAME).setMaster(SPARK_MASTER_URL);
        JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, Durations.seconds(BATCH_DURATION));
        DistributedCache cache = new DistributedCache(jsc.sparkContext());

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        kafkaParams.put("group.id", GROUP_ID);
        kafkaParams.put("enable.auto.commit", "true");
        kafkaParams.put("auto.commit.interval.ms", "1000");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.immunizer.microservices.analyzer.FeatureRecordDeserializer");

        JavaInputDStream<ConsumerRecord<String, FeatureRecord>> consumerRecordStream = 
            KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
            ConsumerStrategies.SubscribePattern(Pattern.compile(TOPIC_PATTERN), kafkaParams));
        JavaDStream<FeatureRecord> featureRecordStream = consumerRecordStream.map(ConsumerRecord::value);
        
        featureRecordStream.foreachRDD(featureRecordRDD -> {
            cache.save(featureRecordRDD);
        });

        // Let the game begin!
        jsc.start();
        jsc.awaitTermination();
    }
}