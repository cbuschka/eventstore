package com.github.cbuschka.eventstore;

import java.util.List;
import java.util.UUID;

public interface EventPagingSupport {

    List<PublishableEvent> getEventsSince(UUID eventUuid, int limit);
}
