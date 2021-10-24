package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.EventStore;
import com.github.cbuschka.eventstore.EventStoreBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.sql.DriverManager;
import java.sql.SQLException;

class JdbcEventStoreIntegrationTest extends AbstractEventStoreTest {

    private static final Logger logger = LoggerFactory.getLogger(JdbcEventStoreIntegrationTest.class);

    private static GenericContainer postgresContainer;

    @BeforeAll
    public static void beforeAll() {
        try {
            DriverManager.getConnection("jdbc:postgresql://localhost:5432/eventstore", "eventstore", "asdfasdf").close();
            logger.info("Postgres already running...");
            return;
        } catch (SQLException ex) {
            // delayed
        }
        
        postgresContainer = new GenericContainer<>(DockerImageName.parse("postgres:14"))
            .withExposedPorts(5432)
            .withPrivilegedMode(false)
            .withReuse(true)
            .withEnv("POSTGRES_USER", "eventstore")
            .withEnv("POSTGRES_PASSWORD", "asdfasdf")
            .withEnv("POSTGRES_DB", "eventstore")
            .withLogConsumer(new Slf4jLogConsumer(logger));
        postgresContainer.start();
    }

    @Override
    protected EventStore createEventStore() {
        int port = postgresContainer == null ? 5432 : postgresContainer.getMappedPort(5432);
        return EventStoreBuilder.jdbc()
            .driverManager(String.format("jdbc:postgresql://localhost:%d/eventstore", port),
                "eventstore", "asdfasdf")
            .build();
    }

    @AfterAll
    public static void afterAll() {
        if (postgresContainer != null) {
            postgresContainer.stop();
        }
    }
}
