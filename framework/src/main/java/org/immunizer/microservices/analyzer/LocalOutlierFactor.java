package org.immunizer.microservices.analyzer;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.outlier.LOF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class LocalOutlierFactor {

    public LocalOutlierFactor(Dataset<Row> df, int minPoints) {
        VectorAssembler assembler = new VectorAssembler().setInputCols(df.columns()).setOutputCol("features");
        Dataset<Row> tdf = assembler.transform(df).repartition(4);
        Dataset<Row> result = new LOF().setMinPts(minPoints).transform(tdf);
    }
}