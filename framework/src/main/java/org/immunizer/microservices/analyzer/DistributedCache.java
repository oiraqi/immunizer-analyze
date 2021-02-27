package org.immunizer.microservices.analyzer;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.StatCounter;

import scala.Tuple2;

public class DistributedCache {

    private JavaIgniteContext<String, FeatureRecord> igniteContext;    
    private JavaIgniteRDD<String, FeatureRecord> featureRecordRDD;

    public DistributedCache(JavaSparkContext sc) {        
        igniteContext = new JavaIgniteContext<String, FeatureRecord>(sc, "immunizer/ignite-cfg.xml");
        featureRecordRDD = igniteContext.fromCache("featureRecords");
    }

    public void save(JavaRDD<FeatureRecord> frRDD) {
        JavaPairRDD<String, FeatureRecord> rowRDD = frRDD.mapToPair(fr -> new Tuple2(fr.getSwid() + '_' + fr.getCallStackId(), fr));
        featureRecordRDD.savePairs(rowRDD);
    }
}