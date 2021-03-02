package org.immunizer.microservices.analyzer;

import org.apache.spark.ml.outlier.LOF;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

import java.util.List;

public class LocalOutlierFactor {

    public LocalOutlierFactor() {
    }

    public List<Row> process(Dataset<Row> df, int minPoints, int topOutliers) {
        return new LOF().setMinPts(minPoints).transform(df)
                        .sort(desc("lof")).takeAsList(topOutliers);
    }
}