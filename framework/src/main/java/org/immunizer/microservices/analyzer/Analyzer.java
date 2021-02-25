package org.immunizer.microservices.analyzer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;

public class Analyzer {

    public static void main(String[] args) {
        
        FeatureRecordConsumer consumer = new FeatureRecordConsumer();

        try {
            while (true) {
                ConsumerRecords<String, FeatureRecord> records = consumer.poll(Duration.ofSeconds(60));
                for (ConsumerRecord<String, FeatureRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }

}