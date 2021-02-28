package org.immunizer.microservices.analyzer;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

import scala.Tuple2;

public class DistributedCache {

    private JavaIgniteContext<Long, FeatureRecord> igniteContext;
    private final int MIN_THRESHOLD = 100;
    private final int MAX_THRESHOLD = 100000;
    private JavaSparkContext sc;

    public DistributedCache(JavaSparkContext sc) { 
        igniteContext = new JavaIgniteContext<Long, FeatureRecord>(sc, "immunizer/ignite-cfg.xml");
        this.sc = sc;
    }

    public void save(String context, List<Tuple2<Long, FeatureRecord>> recordsToBeSaved) {
        JavaPairRDD<Long, FeatureRecord> recordsToBeSavedRDD =
                    sc.parallelizePairs(recordsToBeSaved);
        
        JavaIgniteRDD<Long, FeatureRecord> featureRecordRDD = igniteContext.fromCache("FRC/" + context);
        featureRecordRDD.savePairs(recordsToBeSavedRDD);
    }

    public JavaPairRDD<Long, FeatureRecord> fetch(String context) {
        return igniteContext.fromCache("FRC/" + context);
    }
}