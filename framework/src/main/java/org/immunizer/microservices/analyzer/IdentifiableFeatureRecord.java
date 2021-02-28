package org.immunizer.microservices.analyzer;

import java.util.HashMap;
import java.util.Iterator;
import java.io.Serializable;

public class IdentifiableFeatureRecord {
    private long id;
    private FeatureRecord featureRecord;

    public IdentifiableFeatureRecord(long id, FeatureRecord featureRecord) {
        this.id = id;
        this.featureRecord = featureRecord;
    }

    public long getId() {
        return id;
    }

    public FeatureRecord getFeatureRecord() {
        return featureRecord;
    }
}