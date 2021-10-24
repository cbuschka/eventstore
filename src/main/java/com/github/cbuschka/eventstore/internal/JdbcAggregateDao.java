package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.AlreadyExists;
import com.github.cbuschka.eventstore.StaleData;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class JdbcAggregateDao {

    public boolean existsAggregate(UUID aggregateUuid, Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("select * from aggregate where uuid=?::uuid")) {
            pStmt.setString(1, aggregateUuid.toString());
            try (ResultSet rSet = pStmt.executeQuery()) {
                return rSet.next();
            }
        }
    }

    public void updateAggregate(AggregateRecord aggregateRecord, Integer expectedVersion, AggregateMetaData metaData, Connection conn) throws SQLException, StaleData {
        aggregateRecord.version = aggregateRecord.version + 1;
        try (PreparedStatement pStmt = conn.prepareStatement("update aggregate set version=?, type=?, snapshot=?::jsonb, correlation_uuid=?::uuid where uuid=?::uuid and version=?")) {
            pStmt.setInt(1, aggregateRecord.version);
            pStmt.setString(2, metaData.type);
            pStmt.setString(3, aggregateRecord.snapshot);
            pStmt.setString(4, aggregateRecord.correlationUuid != null ? aggregateRecord.correlationUuid.toString() : null);
            pStmt.setString(5, aggregateRecord.aggregateUuid.toString());
            pStmt.setInt(6, expectedVersion);
            int updateCount = pStmt.executeUpdate();
            if (updateCount == 0) {
                throw new StaleData();
            }
        }
    }

    public void insertAggregate(AggregateRecord aggregateRecord, AggregateMetaData metaData, Connection conn) throws SQLException, AlreadyExists {
        aggregateRecord.version = 0;
        try (PreparedStatement pStmt = conn.prepareStatement("insert into aggregate ( uuid, version, type, snapshot, correlation_uuid ) values ( ?::uuid, ?, ?, ?::jsonb, ?::uuid )")) {
            pStmt.setString(1, aggregateRecord.aggregateUuid.toString());
            pStmt.setInt(2, aggregateRecord.version);
            pStmt.setString(3, metaData.type);
            pStmt.setString(4, aggregateRecord.snapshot);
            pStmt.setString(5, aggregateRecord.correlationUuid != null ? aggregateRecord.correlationUuid.toString() : null);
            pStmt.executeUpdate();
        } catch (SQLException e) {
            if (e.getMessage().contains("duplicate key value violates unique constraint \"ak_agg_corr_uuid\"")) {
                throw new AlreadyExists();
            }

            throw e;
        }
    }

    public void initSchema(Connection conn) throws SQLException {
        try (PreparedStatement pStmt = conn.prepareStatement("create table if not exists aggregate ( id bigserial primary key, uuid uuid not null, version int not null, correlation_uuid uuid, type varchar(80) not null, snapshot jsonb, constraint ak_agg_uuid unique (uuid), constraint ak_agg_corr_uuid unique (correlation_uuid) )")) {
            pStmt.executeUpdate();
        }
    }
}
