package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InMemoryEventStoreTest extends AbstractEventStoreTest {

    @Override
    protected EventStore createEventStore() {
        return EventStoreBuilder.inMemory().build();
    }
}
