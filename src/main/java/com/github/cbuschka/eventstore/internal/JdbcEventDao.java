package com.github.cbuschka.eventstore.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.cbuschka.eventstore.PublishableEvent;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class JdbcEventDao {

    private JsonConverter jsonConverter;

    public JdbcEventDao(JsonConverter jsonConverter) {
        this.jsonConverter = jsonConverter;
    }

    public Set<UUID> selectEventUuidsFor(UUID aggregateUuid, Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("select uuid from event where aggregate_uuid=?::uuid")) {
            pStmt.setString(1, aggregateUuid.toString());
            try (ResultSet rSet = pStmt.executeQuery()) {
                Set<UUID> eventUuids = new HashSet<>();
                while (rSet.next()) {
                    UUID eventUuid = UUID.fromString(rSet.getString("uuid"));
                    eventUuids.add(eventUuid);
                }
                return eventUuids;
            }
        }
    }

    public List<AggregateRecord.EventRecord> selectEventRecordsFor(UUID aggregateUuid, Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("select uuid, type, data from event where aggregate_uuid=?::uuid order by sequence_no asc")) {
            pStmt.setString(1, aggregateUuid.toString());
            try (ResultSet rSet = pStmt.executeQuery()) {
                List<AggregateRecord.EventRecord> eventRecords = new ArrayList<>();
                while (rSet.next()) {
                    UUID eventUuid = UUID.fromString(rSet.getString("uuid"));
                    String type = rSet.getString("type");
                    String data = rSet.getString("data");
                    eventRecords.add(new AggregateRecord.EventRecord(eventUuid, type, data));
                }
                return eventRecords;
            }
        }
    }

    public void insertEvents(AggregateRecord aggregateRecord, Connection conn) throws SQLException {

        Set<UUID> persistentEventUuids = selectEventUuidsFor(aggregateRecord.aggregateUuid, conn);

        List<AggregateRecord.EventRecord> events = aggregateRecord.events;
        UUID aggregateUuid = aggregateRecord.aggregateUuid;
        try (PreparedStatement pStmt = conn.prepareStatement("insert into event ( uuid, aggregate_uuid, sequence_no, type, data ) values ( ?::uuid, ?::uuid, ?, ?, ?::jsonb)")) {
            for (int i = 0; i < events.size(); ++i) {
                AggregateRecord.EventRecord eventRecord = events.get(i);
                if (persistentEventUuids.contains(eventRecord.eventUUID)) {
                    continue;
                }

                pStmt.setString(1, eventRecord.eventUUID.toString());
                pStmt.setString(2, aggregateUuid.toString());
                pStmt.setInt(3, i);
                pStmt.setString(4, eventRecord.type);
                pStmt.setString(5, eventRecord.data);
                pStmt.addBatch();
            }
            pStmt.executeBatch();
        }
    }

    public List<PublishableEvent> selectUnpublishedEventsFor(int limit, Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("select uuid, type, data from event where published_at is null order by aggregate_uuid, sequence_no asc limit ?")) {
            pStmt.setInt(1, limit);
            try (ResultSet rSet = pStmt.executeQuery()) {
                List<PublishableEvent> events = new ArrayList<>();
                while (rSet.next()) {
                    UUID eventUuid = UUID.fromString(rSet.getString("uuid"));
                    String type = rSet.getString("type");
                    String dataJson = rSet.getString("data");
                    JsonNode data = jsonConverter.fromJson(dataJson, JsonNode.class);
                    events.add(new PublishableEvent(eventUuid, type, data));
                }
                return events;
            }
        }
    }

    public void updatePublishedAtFor(List<UUID> uuids, Connection conn) throws SQLException {
        if (uuids.isEmpty()) {
            return;
        }

        try (PreparedStatement pStmt = conn.prepareStatement("update event set published_at = now() where uuid in ( ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid, ?::uuid ) and published_at is null")) {
            for (int i = 0; i < (uuids.size() / 10 + 1) * 10; ++i) {
                pStmt.setString(i % 10 + 1, uuids.get(i < uuids.size() ? i : uuids.size() - 1).toString());
                if (i % 10 == 9) {
                    pStmt.addBatch();
                }
            }
            pStmt.executeBatch();
        }
    }

    public List<PublishableEvent> selectEventsSinceFor(UUID optionalEventUuid, int limit, Connection conn) throws SQLException {

        if (optionalEventUuid == null) {
            try (PreparedStatement pStmt = conn.prepareStatement("select uuid, type, data from event order by id asc limit ?")) {
                pStmt.setInt(1, limit);
                try (ResultSet rSet = pStmt.executeQuery()) {
                    List<PublishableEvent> events = new ArrayList<>();
                    while (rSet.next()) {
                        UUID uuid = UUID.fromString(rSet.getString("uuid"));
                        String type = rSet.getString("type");
                        String dataJson = rSet.getString("data");
                        JsonNode data = jsonConverter.fromJson(dataJson, JsonNode.class);
                        events.add(new PublishableEvent(uuid, type, data));
                    }
                    return events;
                }
            }
        } else {
            long minId;
            try (PreparedStatement pStmt = conn.prepareStatement("select id from event where uuid = ?::uuid")) {
                pStmt.setString(1, optionalEventUuid.toString());
                try (ResultSet rSet = pStmt.executeQuery()) {
                    if (!rSet.next()) {
                        throw new IllegalArgumentException("Start event " + optionalEventUuid + " not found.");
                    }
                    minId = rSet.getLong("id");
                }
            }

            try (PreparedStatement pStmt = conn.prepareStatement("select uuid, type, data from event where id > ? order by id limit ?")) {
                pStmt.setLong(1, minId);
                pStmt.setInt(2, limit);
                try (ResultSet rSet = pStmt.executeQuery()) {
                    List<PublishableEvent> events = new ArrayList<>();
                    while (rSet.next()) {
                        UUID uuid = UUID.fromString(rSet.getString("uuid"));
                        String type = rSet.getString("type");
                        String dataJson = rSet.getString("data");
                        JsonNode data = jsonConverter.fromJson(dataJson, JsonNode.class);
                        events.add(new PublishableEvent(uuid, type, data));
                    }
                    return events;
                }
            }
        }
    }

    public void initSchema(Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("create table if not exists event ( id bigserial primary key, uuid uuid not null, aggregate_uuid uuid not null, sequence_no int not null, type varchar(80) not null, data jsonb, published_at timestamp, constraint ak_event_uuid unique ( uuid ), constraint fk_agg_uuid foreign key ( aggregate_uuid ) references aggregate ( uuid ), constraint ak_event_agg_uuid_seq_no unique ( aggregate_uuid, sequence_no ) )")) {
            pStmt.executeUpdate();
        }
    }
}
