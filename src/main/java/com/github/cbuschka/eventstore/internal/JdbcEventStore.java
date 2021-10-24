package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.*;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class JdbcEventStore extends AbstractEventStore implements EventPublishingSupport, EventPagingSupport {

    private ConnectionProvider dataSource;

    private JdbcAggregateDao aggregateDao = new JdbcAggregateDao();
    private JdbcEventDao eventDao;

    public JdbcEventStore(ConnectionProvider dataSource) {
        this(dataSource, new JsonConverter());
    }

    private JdbcEventStore(ConnectionProvider dataSource, JsonConverter jsonConverter) {
        super(jsonConverter);
        this.dataSource = dataSource;
        this.eventDao = new JdbcEventDao(jsonConverter);
    }

    @SneakyThrows(SQLException.class)
    public void initSchema() {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            aggregateDao.initSchema(conn);
            eventDao.initSchema(conn);
            conn.commit();
        }
    }

    @SneakyThrows(SQLException.class)
    @Override
    protected void saveAggregate(AggregateRecord aggregateRecord, Integer expectedVersion, AggregateMetaData metaData) throws StaleData, AlreadyExists {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            if (!aggregateDao.existsAggregate(aggregateRecord.aggregateUuid, conn)) {
                aggregateDao.insertAggregate(aggregateRecord, metaData, conn);
            } else {
                aggregateDao.updateAggregate(aggregateRecord, expectedVersion, metaData, conn);
            }

            eventDao.insertEvents(aggregateRecord, conn);

            conn.commit();
        }
    }

    @SneakyThrows(SQLException.class)
    @Override
    protected AggregateRecord loadAggregate(UUID aggregateUuid, AggregateMetaData metaData) throws NoSuchAggregate {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            AggregateRecord aggregateRecord;
            try (PreparedStatement pStmt = conn.prepareStatement("select uuid, version, snapshot::varchar, correlation_uuid from aggregate where uuid=?::uuid")) {
                pStmt.setString(1, aggregateUuid.toString());
                try (ResultSet rSet = pStmt.executeQuery()) {
                    boolean next = rSet.next();
                    if (!next) {
                        throw new NoSuchAggregate();
                    }
                    int version = rSet.getInt("version");
                    String snapshot = rSet.getString("snapshot");
                    String correlationUuid = rSet.getString("correlation_uuid");
                    conn.rollback();

                    aggregateRecord = new AggregateRecord(aggregateUuid, correlationUuid != null ? UUID.fromString(correlationUuid) : null, version, snapshot, new ArrayList<>());
                }
            }

            aggregateRecord.events = eventDao.selectEventRecordsFor(aggregateUuid, conn);

            return aggregateRecord;
        }
    }

    @SneakyThrows(SQLException.class)
    @Override
    public List<PublishableEvent> getUnpublishedEvents(int limit) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            List<PublishableEvent> events = eventDao.selectUnpublishedEventsFor(limit, conn);
            conn.rollback();
            return events;
        }
    }

    @SneakyThrows(SQLException.class)
    @Override
    public void markPublished(List<PublishableEvent> events) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            List<UUID> eventUuids = events.stream().map(PublishableEvent::getUuid).collect(Collectors.toList());
            eventDao.updatePublishedAtFor(eventUuids, conn);
            conn.commit();
        }
    }

    @SneakyThrows(SQLException.class)
    @Override
    public List<PublishableEvent> getEventsSince(UUID eventUuid, int limit) {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            List<PublishableEvent> events = eventDao.selectEventsSinceFor(eventUuid, limit, conn);
            conn.rollback();
            return events;
        }
    }
}
