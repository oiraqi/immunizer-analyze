package org.immunizer.microservices.analyzer;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.outlier.LOF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class LocalOutlierFactor {

    private Dataset<Row> tdf;
    private int minPoints;
    private int topOutliers;

    public LocalOutlierFactor(Dataset<Row> df, int minPoints, int topOutliers) {
        VectorAssembler assembler = new VectorAssembler().setInputCols(df.columns()).setOutputCol("features");
        tdf = assembler.transform(df);
    }

    public List<Row> process() {
        return new LOF().setMinPts(minPoints).transform(tdf)
                        .sort(desc("lof")).takeAsList(topOutliers);
    }
}