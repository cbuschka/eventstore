package com.github.cbuschka.eventstore;

import java.util.List;

public interface EventPublishingSupport {

    List<PublishableEvent> getUnpublishedEvents(int limit);

    void markPublished(List<PublishableEvent> events);
}
