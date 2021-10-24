package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.EventStore;
import com.github.cbuschka.eventstore.EventStoreBuilder;

class JdbcEventStoreIntegrationTest extends AbstractEventStoreTest {

    @Override
    protected EventStore createEventStore() {
        return EventStoreBuilder.jdbc()
            .driverManager("jdbc:postgresql://localhost:5432/eventstore", "eventstore", "asdfasdf")
            .build();
    }
}
