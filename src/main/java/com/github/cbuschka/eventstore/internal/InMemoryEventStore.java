package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.AlreadyExists;
import com.github.cbuschka.eventstore.NoSuchAggregate;
import com.github.cbuschka.eventstore.StaleData;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class InMemoryEventStore extends AbstractJsonBlobEventStore {

    private final Map<UUID, Record> dataByAggregateUuid = new HashMap<>();
    private final Map<UUID, UUID> aggregateUuidByCorrelationUuid = new HashMap<>();

    public InMemoryEventStore() {
        super(new JsonConverter());
    }

    @AllArgsConstructor
    @NoArgsConstructor
    private static class Record {
        private String json;
        private int version;
    }

    @Override
    protected synchronized void writeJsonBlob(UUID aggregateUuid, int version, UUID correlationUuid, String json, int expectedVersion, AggregateMetaData metaData) throws StaleData, AlreadyExists {

        Record record = this.dataByAggregateUuid.get(aggregateUuid);
        if (expectedVersion != 0 && (record != null && record.version != expectedVersion)) {
            throw new StaleData();
        }

        UUID correlatedAggregateUuid = this.aggregateUuidByCorrelationUuid.get(correlationUuid);
        if (correlatedAggregateUuid != null && !correlatedAggregateUuid.equals(aggregateUuid)) {
            throw new AlreadyExists();
        }

        Record newRecord = new Record(json, version);
        this.dataByAggregateUuid.put(aggregateUuid, newRecord);
        this.aggregateUuidByCorrelationUuid.put(correlationUuid, aggregateUuid);
    }

    @Override
    protected synchronized String readJsonBlob(UUID aggregateUuid, AggregateMetaData metaData) throws NoSuchAggregate {

        Record record = this.dataByAggregateUuid.get(aggregateUuid);
        if (record == null) {
            throw new NoSuchAggregate();
        }

        return record.json;
    }
}
