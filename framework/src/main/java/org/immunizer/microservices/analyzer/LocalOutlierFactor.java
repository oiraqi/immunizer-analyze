package org.immunizer.microservices.analyzer;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.outlier.LOF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class LocalOutlierFactor {

    private Dataset<Row> tdf;

    public LocalOutlierFactor(Dataset<Row> df) {
        VectorAssembler assembler = new VectorAssembler().setInputCols(df.columns()).setOutputCol("features");
        tdf = assembler.transform(df);
    }

    public List<Row> process(int minPoints, int topOutliers) {
        return new LOF().setMinPts(minPoints).transform(tdf)
                        .sort(desc("lof")).takeAsList(topOutliers);
    }
}