package org.immunizer.microservices.analyzer;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.Vector;

import scala.Tuple2;

public class DistributedCache {

    private JavaIgniteContext<Long, FeatureRecord> igniteContext;
    private JavaSparkContext sc;

    public DistributedCache(JavaSparkContext sc) { 
        igniteContext = new JavaIgniteContext<Long, FeatureRecord>(sc, "immunizer/ignite-cfg.xml");
        this.sc = sc;
    }

    public void save(String context, JavaPairRDD<Long, FeatureRecord> recordsToBeSavedRDD) {        
        JavaIgniteRDD<Long, FeatureRecord> featureRecordRDD = igniteContext.fromCache("FRC/" + context);
        featureRecordRDD.savePairs(recordsToBeSavedRDD);
    }

    public JavaPairRDD<Long, FeatureRecord> fetch(String context) {
        return igniteContext.fromCache("FRC/" + context);
    }

    public void purge(String context, long minKey) {
        igniteContext.fromCache("FRC/" + context).sql("delete from FRC/? where _key < ?", context, minKey);
    }

    public void delete(String context, long key) {
        igniteContext.fromCache("FRC/" + context).sql("delete from FRC/? where _key = ?", context, key);
    }
}