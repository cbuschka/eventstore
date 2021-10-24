package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.*;

import java.util.List;
import java.util.UUID;

public abstract class AbstractJsonBlobEventStore extends AbstractEventStore implements EventStore {

    protected abstract String readJsonBlob(UUID aggregateUuid, AggregateMetaData metaData) throws NoSuchAggregate;

    protected abstract void writeJsonBlob(UUID aggregateUuid, int version, UUID correlationUuid, String json, int expectedVersion, AggregateMetaData metaData) throws StaleData, AlreadyExists;

    protected AbstractJsonBlobEventStore(JsonConverter jsonConverter) {
        super(jsonConverter);
    }

    @Override
    protected void saveAggregate(AggregateRecord aggregateRecord, Integer expectedVersion, AggregateMetaData metaData) throws StaleData, AlreadyExists {
        if (aggregateRecord.aggregateUuid == null) {
            aggregateRecord.aggregateUuid = UUID.randomUUID();
        }
        for (AggregateRecord.EventRecord eventRecord : aggregateRecord.events) {
            if (eventRecord.eventUUID == null) {
                eventRecord.eventUUID = UUID.randomUUID();
            }
        }
        aggregateRecord.version = aggregateRecord.version + 1;
        String json = jsonConverter.toJson(aggregateRecord);
        writeJsonBlob(aggregateRecord.aggregateUuid, aggregateRecord.version, aggregateRecord.correlationUuid, json, expectedVersion, metaData);
    }

    @Override
    protected AggregateRecord loadAggregate(UUID aggregateUuid, AggregateMetaData metaData) throws NoSuchAggregate {
        String json = readJsonBlob(aggregateUuid, metaData);
        return jsonConverter.fromJson(json, AggregateRecord.class);
    }
}
