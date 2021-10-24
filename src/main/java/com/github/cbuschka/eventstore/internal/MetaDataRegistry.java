package com.github.cbuschka.eventstore.internal;

import java.util.HashMap;
import java.util.Map;

public class MetaDataRegistry {

    private Map<Class<?>, AggregateMetaData> metaDataMap = new HashMap<>();

    public synchronized AggregateMetaData getMetaData(Class<?> aggregateType) {

        AggregateMetaData metaData = metaDataMap.get(aggregateType);
        if (metaData == null) {
            metaData = new AggregateMetaData(aggregateType);
            metaDataMap.put(aggregateType, metaData);
        }
        return metaData;
    }
}
