package com.github.cbuschka.eventstore.internal;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
public class AggregateRecord {

    public UUID aggregateUuid;

    public UUID correlationUuid;

    public int version;

    public String snapshot;

    public List<EventRecord> events = new ArrayList<>();

    @NoArgsConstructor
    @AllArgsConstructor
    public static class EventRecord {

        public UUID eventUUID;

        public String type;

        public String data;
    }
}
