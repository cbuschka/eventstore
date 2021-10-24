package com.github.cbuschka.eventstore.internal;

import lombok.SneakyThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class AggregateAccessor {
    private Object aggregateInstance;
    private AggregateMetaData metaData;

    public AggregateAccessor(Object aggregate, AggregateMetaData metaData) {
        this.aggregateInstance = aggregate;
        this.metaData = metaData;
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public static AggregateAccessor newInstance(AggregateMetaData metaData) {
        AggregateAccessor holder = new AggregateAccessor(null, metaData);
        holder.createAggregateInstance();
        if (metaData.eventListField.get(holder.aggregateInstance) == null) {
            metaData.eventListField.set(holder.aggregateInstance, new ArrayList<>());
        }
        return holder;
    }

    public static AggregateAccessor wrap(Object aggregate, AggregateMetaData metaData) {
        return new AggregateAccessor(aggregate, metaData);
    }

    private void createAggregateInstance() throws InstantiationException, IllegalAccessException {
        this.aggregateInstance = this.metaData.aggregateClass.newInstance();
    }


    @SneakyThrows(ReflectiveOperationException.class)
    public void setCorrelationUuidFieldIfExists(UUID correlationUuid) {
        if (this.metaData.aggregateCorrelationUuidField == null) {
            return;
        }

        this.metaData.aggregateCorrelationUuidField.set(this.aggregateInstance, correlationUuid);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public UUID getCorrelationUuidFieldIfExists() {
        if (this.metaData.aggregateCorrelationUuidField == null) {
            return null;
        }

        return (UUID) this.metaData.aggregateCorrelationUuidField.get(this.aggregateInstance);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public UUID getAggregateUuid() {
        Field aggregateUuidField = this.metaData.aggregateUuidField;
        return (UUID) aggregateUuidField.get(this.aggregateInstance);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public List<?> getEvents() {
        Field eventListField = this.metaData.eventListField;
        return (List<?>) eventListField.get(this.aggregateInstance);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public int getVersion() {
        Field aggregateVersionField = this.metaData.aggregateVersionField;
        return (Integer) aggregateVersionField.get(this.aggregateInstance);
    }

    public void replayEvents() {
        List<?> events = getEvents();
        setEvents(new ArrayList<>());
        for (Object event : events) {
            applyEvent(event);
        }
        setEvents(events);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public void applyEvent(Object event) {
        String type = this.metaData.getEventType(event.getClass());
        AggregateMetaData.EventMetaData eventMetaData = this.metaData.eventMap.get(type);
        Method handlerMethod = eventMetaData.handlerMethod;
        handlerMethod.invoke(this.aggregateInstance, event);
    }

    public Object getAggregateInstance() {
        return aggregateInstance;
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public void setAggregateUuid(UUID aggregateUuid) {
        this.metaData.aggregateUuidField.set(this.aggregateInstance, aggregateUuid);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public void setAggregateVersion(int version) {
        this.metaData.aggregateVersionField.set(this.aggregateInstance, version);
    }

    @SneakyThrows(ReflectiveOperationException.class)
    public void setEvents(List<?> events) {
        this.metaData.eventListField.set(this.aggregateInstance, events);
    }
}
