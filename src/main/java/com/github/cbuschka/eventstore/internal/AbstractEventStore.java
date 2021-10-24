package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.AlreadyExists;
import com.github.cbuschka.eventstore.EventStore;
import com.github.cbuschka.eventstore.NoSuchAggregate;
import com.github.cbuschka.eventstore.StaleData;
import lombok.SneakyThrows;

import java.util.UUID;

import static java.util.stream.Collectors.toList;

public abstract class AbstractEventStore implements EventStore {

    private final MetaDataRegistry metaDataRegistry = new MetaDataRegistry();

    protected JsonConverter jsonConverter;

    protected AbstractEventStore(JsonConverter jsonConverter) {
        this.jsonConverter = jsonConverter;
    }

    @Override
    public <A> A fetchAggregate(UUID aggregateUuid, Class<A> aggregateType) throws NoSuchAggregate {

        AggregateMetaData metaData = this.metaDataRegistry.getMetaData(aggregateType);
        AggregateRecord aggregateRecord = loadAggregate(aggregateUuid, metaData);
        AggregateAccessor aggregateHolder;
        if (metaData.snaphotEnabled && aggregateRecord.snapshot != null) {
            Object aggregate = this.jsonConverter.fromJson(aggregateRecord.snapshot, metaData.aggregateClass);
            aggregateHolder = AggregateAccessor.wrap(aggregate, metaData);
            aggregateHolder.setEvents(aggregateRecord.events.stream().map((er) -> toEvent(er, metaData)).collect(toList()));
        } else {
            aggregateHolder = AggregateAccessor.newInstance(metaData);
            aggregateHolder.setEvents(aggregateRecord.events.stream().map((er) -> toEvent(er, metaData)).collect(toList()));
            aggregateHolder.replayEvents();
        }
        aggregateHolder.setAggregateUuid(aggregateUuid);
        aggregateHolder.setAggregateVersion(aggregateRecord.version);
        aggregateHolder.setCorrelationUuidFieldIfExists(aggregateRecord.correlationUuid);
        return (A) aggregateHolder.getAggregateInstance();
    }

    @SneakyThrows(ReflectiveOperationException.class)
    private Object toEvent(AggregateRecord.EventRecord eventRecord, AggregateMetaData aggregateMetaData) {

        AggregateMetaData.EventMetaData eventMetaData = aggregateMetaData.eventMap.get(eventRecord.type);
        Class<?> eventClass = eventMetaData.eventClass;
        Object event = this.jsonConverter.fromJson(eventRecord.data, eventClass);
        eventMetaData.eventUuidField.set(event, eventRecord.eventUUID);
        return event;
    }

    @SneakyThrows(ReflectiveOperationException.class)
    private AggregateRecord.EventRecord toEventRecord(Object event, AggregateMetaData aggregateMetaData) {

        AggregateRecord.EventRecord eventRecord = new AggregateRecord.EventRecord();
        String eventType = aggregateMetaData.getEventType(event.getClass());
        AggregateMetaData.EventMetaData eventMetaData = aggregateMetaData.eventMap.get(eventType);
        eventRecord.type = eventMetaData.type;
        UUID eventUuid = (UUID) eventMetaData.eventUuidField.get(event);
        if (eventUuid == null) {
            eventUuid = UUID.randomUUID();
            eventMetaData.eventUuidField.set(event, eventUuid);
        }
        eventRecord.eventUUID = eventUuid;
        eventRecord.data = this.jsonConverter.toJson(event);
        return eventRecord;
    }

    @Override
    public <A> UUID storeAggregate(A aggregate) throws StaleData, AlreadyExists {

        AggregateAccessor aggregateHolder = AggregateAccessor.wrap(aggregate, new AggregateMetaData(aggregate.getClass()));
        AggregateMetaData metaData = this.metaDataRegistry.getMetaData(aggregate.getClass());
        UUID aggregateUuid = aggregateHolder.getAggregateUuid();
        if (aggregateUuid == null) {
            aggregateUuid = UUID.randomUUID();
            aggregateHolder.setAggregateUuid(aggregateUuid);
        }
        int oldVersion = aggregateHolder.getVersion();
        AggregateRecord aggregateRecord = new AggregateRecord();
        aggregateRecord.aggregateUuid = aggregateUuid;
        aggregateRecord.version = aggregateHolder.getVersion();
        aggregateRecord.correlationUuid = aggregateHolder.getCorrelationUuidFieldIfExists();
        aggregateRecord.events = aggregateHolder.getEvents()
            .stream()
            .map((e) -> toEventRecord(e, metaData))
            .collect(toList());
        aggregateRecord.version = aggregateHolder.getVersion();

        if (metaData.snaphotEnabled) {
            aggregateRecord.snapshot = this.jsonConverter.toJson(aggregate);
        }

        saveAggregate(aggregateRecord, oldVersion, metaData);

        aggregateHolder.setAggregateVersion(aggregateRecord.version);
        aggregateHolder.setEvents(aggregateRecord.events.stream().map((er) -> toEvent(er, metaData)).collect(toList()));
        return aggregateRecord.aggregateUuid;
    }

    protected abstract void saveAggregate(AggregateRecord aggregateRecord, Integer expectedVersion, AggregateMetaData metaData) throws StaleData, AlreadyExists;

    protected abstract AggregateRecord loadAggregate(UUID aggregateUuid, AggregateMetaData metaData) throws NoSuchAggregate;
}
