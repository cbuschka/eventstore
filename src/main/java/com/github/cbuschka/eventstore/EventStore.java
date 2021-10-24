package com.github.cbuschka.eventstore;

import java.util.List;
import java.util.UUID;

public interface EventStore {

    <A> A fetchAggregate(UUID aggregateUuid, Class<A> aggregateType) throws NoSuchAggregate;

    <A> UUID storeAggregate(A aggregate) throws StaleData, AlreadyExists;
}
