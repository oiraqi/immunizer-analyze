package org.immunizer.microservices.analyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.ml.linalg.VectorUDT;

import java.util.List;
import java.util.Iterator;

import scala.Tuple2;

public class Analyzer {

    private static final String SPARK_MASTER_URL = "spark://spark-master:7077";
    private static final String APP_NAME = "Analyzer";
    private static final int BATCH_DURATION = 60;
    private static final int MIN_BATCH_SIZE = 100;
    private static final int MAX_BATCH_SIZE = 100000;
    private static final int MIN_POINTS = 1000;
    private static final int TOP_OUTLIERS = 10;

    public static void main(String[] args) throws Exception {
        SparkSession sparkSession = SparkSession.builder().appName(APP_NAME).master(SPARK_MASTER_URL).getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
        DistributedCache cache = new DistributedCache(sc);
        FeatureRecordConsumer consumer = new FeatureRecordConsumer(sc, cache);
        OutlierProducer producer = new OutlierProducer();
        LocalOutlierFactor localOutlierFactor = new LocalOutlierFactor();
        StructType structType = new StructType();                    
        structType.add("id", DataTypes.LongType);
        structType.add("features", new VectorUDT());

        try {
            while(true) {
                Iterator<String> contexts =
                    consumer.poll(BATCH_DURATION, MIN_BATCH_SIZE, MAX_BATCH_SIZE);

                while(contexts.hasNext()) {
                    String context = contexts.next();
                    JavaPairRDD<Long, FeatureRecord> fetchedRecordsRDD = cache.fetch(context);                    
                    JavaRDD<Row> rowRDD = fetchedRecordsRDD.map(record -> {
                        return RowFactory.create(record._1, record._2.getRecord().values());
                    });

                    Dataset<Row> df = sparkSession.createDataFrame(rowRDD, structType);
                    List<Row> outliers = localOutlierFactor.process(df, MIN_POINTS, TOP_OUTLIERS);
                    outliers.forEach(outlier -> {
                        FeatureRecord fr = fetchedRecordsRDD.filter(rec -> 
                            rec._1 == outlier.get(0)).map(rec -> rec._2).first();
                        producer.send(fr);
                    });
                }
            }
        } finally {
          consumer.close();
          producer.close();
        }
    }
}