package org.immunizer.microservices.analyzer;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import java.util.Vector;

import scala.Tuple2;

public class DistributedCache {

    private JavaIgniteContext<String, FeatureRecord> igniteContext;    
    private JavaIgniteRDD<String, FeatureRecord> featureRecordRDD;
    private final int MIN_THRESHOLD = 100;
    private final int MAX_THRESHOLD = 100000;

    public DistributedCache(JavaSparkContext sc) {        
        igniteContext = new JavaIgniteContext<String, FeatureRecord>(sc, "immunizer/ignite-cfg.xml");
        featureRecordRDD = igniteContext.fromCache("featureRecords");
    }

    public void save(JavaPairRDD<String, IdentifiableFeatureRecord> frRDD) {
        JavaPairRDD<String, FeatureRecord> recordsToBeSaved = 
            frRDD.mapToPair(record -> 
                new Tuple2<String, FeatureRecord>(
                    record._1() + '_' + record._2().getId(), record._2().getFeatureRecord()));
        featureRecordRDD.savePairs(recordsToBeSaved);
    }

    public JavaIgniteRDD<String, FeatureRecord> fetch(String context, long count) {
        return null;
    }
}