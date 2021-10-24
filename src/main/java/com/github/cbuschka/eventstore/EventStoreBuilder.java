package com.github.cbuschka.eventstore;

import com.github.cbuschka.eventstore.internal.ConnectionProvider;
import com.github.cbuschka.eventstore.internal.InMemoryEventStore;
import com.github.cbuschka.eventstore.internal.JdbcEventStore;

import javax.sql.DataSource;
import java.sql.DriverManager;

public class EventStoreBuilder {

    public static InMemoryEventStoreBuilder inMemory() {
        return new InMemoryEventStoreBuilder();
    }

    public static JdbcEventStoreBuilder jdbc() {
        return new JdbcEventStoreBuilder();
    }

    public static class JdbcEventStoreBuilder {

        private ConnectionProvider connectionProvider;

        public JdbcEventStoreBuilder dataSource(DataSource dataSource) {
            this.connectionProvider = dataSource::getConnection;
            return this;
        }

        public JdbcEventStoreBuilder driverManager(String url, String username, String password) {
            this.connectionProvider = () -> DriverManager.getConnection(url, username, password);
            return this;
        }

        public EventStore build() {
            if (this.connectionProvider == null) {
                throw new IllegalStateException("You must defined a connection provider.");
            }

            JdbcEventStore jdbcEventStore = new JdbcEventStore(this.connectionProvider);
            jdbcEventStore.initSchema();
            return jdbcEventStore;
        }
    }


    public static class InMemoryEventStoreBuilder {
        private InMemoryEventStoreBuilder() {
        }

        public EventStore build() {
            return new InMemoryEventStore();
        }
    }
}
