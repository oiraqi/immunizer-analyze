package org.immunizer.microservices.analyzer;

import org.apache.ignite.spark.JavaIgniteContext;
import org.apache.ignite.spark.JavaIgniteRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.ml.linalg.VectorUDT;

import java.util.List;
import java.util.Vector;

import scala.Tuple2;

public class DistributedCache {

    private JavaIgniteContext<Long, FeatureRecord> igniteContext;
    private SparkSession sparkSession;
    private JavaSparkContext sc;
    private StructType structType;

    public DistributedCache(SparkSession sparkSession) {   
        this.sparkSession = sparkSession;     
        sc = new JavaSparkContext(sparkSession.sparkContext());
        igniteContext = new JavaIgniteContext<Long, FeatureRecord>(sc, "immunizer/ignite-cfg.xml");
        structType = new StructType();
        structType.add("id", DataTypes.LongType);
        structType.add("features", new VectorUDT());
    }

    public void save(String context, JavaPairRDD<Long, FeatureRecord> recordsToBeSavedRDD) {        
        JavaIgniteRDD<Long, FeatureRecord> featureRecordRDD = igniteContext.fromCache("FRC/" + context);
        featureRecordRDD.savePairs(recordsToBeSavedRDD);
    }

    public Dataset<Row> fetch(String context) {
        JavaPairRDD<Long, FeatureRecord> fetchedRecordsRDD =
            igniteContext.fromCache("FRC/" + context);
        JavaRDD<Row> rowRDD = fetchedRecordsRDD.map(record -> {
            return RowFactory.create(record._1, record._2.getRecord().values());
        });

        return sparkSession.createDataFrame(rowRDD, structType);
    }

    public FeatureRecord get(String context, long key) {
        JavaPairRDD<Long, FeatureRecord> fetchedRecordsRDD =
                        igniteContext.fromCache("FRC/" + context);
        return fetchedRecordsRDD.filter(rec -> 
            rec._1 == key).map(rec -> rec._2).first();
    }

    public void purge(String context, long minKey) {
        igniteContext.fromCache("FRC/" + context).sql("delete from FRC/? where _key < ?", context, minKey);
    }

    public void delete(String context, long key) {
        igniteContext.fromCache("FRC/" + context).sql("delete from FRC/? where _key = ?", context, key);
    }
}