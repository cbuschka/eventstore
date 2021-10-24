package com.github.cbuschka.eventstore.internal;

import com.github.cbuschka.eventstore.*;
import example.Order;
import example.OrderState;
import example_with_correlation.Contract;
import example_with_correlation.ContractState;
import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

abstract class AbstractEventStoreTest {

    private static final String ORDER_NO = "O1234";
    private static final String CONTRACT_NO = "C1234";
    private EventStore eventStore;

    @BeforeEach
    public void setUp() {
        this.eventStore = createEventStore();
    }

    protected abstract EventStore createEventStore();

    @Test
    public void detectsUnknownAggregate() {

        assertThatThrownBy(() -> {
            eventStore.fetchAggregate(UUID.randomUUID(), Contract.class);
        }).isInstanceOf(NoSuchAggregate.class);
    }

    @Test
    public void enforcesCorrelationUuidUnique() throws StaleData, AlreadyExists {

        Order order0 = new Order();
        UUID placeOrderEventUuid = UUID.randomUUID();
        order0.place(placeOrderEventUuid, ORDER_NO);
        eventStore.storeAggregate(order0);

        assertThatThrownBy(() -> {
            Order order1 = new Order();
            order1.place(placeOrderEventUuid, ORDER_NO);
            eventStore.storeAggregate(order1);
        }).isInstanceOf(AlreadyExists.class);
    }

    @Test
    public void detectsStaleDataWithCorrelation() throws StaleData, NoSuchAggregate, AlreadyExists {

        UUID orderUuid = eventStore.storeAggregate(new Order());

        Order order1 = eventStore.fetchAggregate(orderUuid, Order.class);
        Order order2 = eventStore.fetchAggregate(orderUuid, Order.class);
        UUID placeOrderEventUuid = UUID.randomUUID();
        order2.place(placeOrderEventUuid, ORDER_NO);
        eventStore.storeAggregate(order2);

        assertThatThrownBy(() -> {
            eventStore.storeAggregate(order1);
        }).isInstanceOf(StaleData.class);
    }

    @Test
    public void detectsStaleDataWithoutCorrelation() throws StaleData, NoSuchAggregate, AlreadyExists {

        UUID contractUuid = eventStore.storeAggregate(new Contract());

        Contract contract1 = eventStore.fetchAggregate(contractUuid, Contract.class);
        Contract contract2 = eventStore.fetchAggregate(contractUuid, Contract.class);
        contract2.sign(CONTRACT_NO);
        eventStore.storeAggregate(contract2);

        assertThatThrownBy(() -> {
            eventStore.storeAggregate(contract1);
        }).isInstanceOf(StaleData.class);
    }

    @Test
    public void turnAroundWithoutCorrelation() throws StaleData, NoSuchAggregate, AlreadyExists {

        UUID orderUuid = eventStore.storeAggregate(new Contract());

        Contract contract = eventStore.fetchAggregate(orderUuid, Contract.class);
        assertThat(contract.getState()).isEqualTo(ContractState.NEW);
        contract.sign(CONTRACT_NO);
        assertThat(contract.getState()).isEqualTo(ContractState.SIGNED);
        eventStore.storeAggregate(contract);

        contract = eventStore.fetchAggregate(orderUuid, Contract.class);
        assertThat(contract.getState()).isEqualTo(ContractState.SIGNED);
        assertThat(contract.getContractNo()).isEqualTo(CONTRACT_NO);
        contract.cancel();
        eventStore.storeAggregate(contract);
    }

    @Test
    public void turnAroundWithCorrelation() throws StaleData, NoSuchAggregate, AlreadyExists {

        UUID orderUuid = eventStore.storeAggregate(new Order());

        Order order = eventStore.fetchAggregate(orderUuid, Order.class);
        assertThat(order.getState()).isEqualTo(OrderState.NEW);
        UUID placeOrderEventUuid = UUID.randomUUID();
        order.place(placeOrderEventUuid, ORDER_NO);
        assertThat(order.getState()).isEqualTo(OrderState.PLACED);
        eventStore.storeAggregate(order);

        order = eventStore.fetchAggregate(orderUuid, Order.class);
        assertThat(order.getState()).isEqualTo(OrderState.PLACED);
        assertThat(order.getOrderNo()).isEqualTo(ORDER_NO);
        order.cancel();
        eventStore.storeAggregate(order);
    }

    @Test
    public void providesPublishableEvents() throws StaleData, AlreadyExists {
        assumeThat(this.eventStore).isInstanceOf(EventPublishingSupport.class);

        List<PublishableEvent> events;
        do {
            events = ((EventPublishingSupport) eventStore).getUnpublishedEvents(1000);
            ((EventPublishingSupport) eventStore).markPublished(events);
        } while (!events.isEmpty());


        Order order = new Order();
        UUID placeOrderEventUuid = UUID.randomUUID();
        order.place(placeOrderEventUuid, ORDER_NO);
        eventStore.storeAggregate(order);

        events = ((EventPublishingSupport) eventStore).getUnpublishedEvents(100);
        assertThat(events.stream().map(PublishableEvent::getType).collect(Collectors.toSet())).contains("OrderPlaced");

        ((EventPublishingSupport) eventStore).markPublished(events);
        events = ((EventPublishingSupport) eventStore).getUnpublishedEvents(100);
        assertThat(events).isEmpty();
    }

    @Test
    public void providesPagedEvents() throws StaleData, AlreadyExists {
        assumeThat(this.eventStore).isInstanceOf(EventPagingSupport.class);

        UUID lastEventUuid = null;

        List<PublishableEvent> events;
        do {
            events = ((EventPagingSupport) eventStore).getEventsSince(lastEventUuid, 1000);
            if (!events.isEmpty()) {
                lastEventUuid = events.get(events.size() - 1).getUuid();
            }
        } while (!events.isEmpty());

        Order order = new Order();
        UUID placeOrderEventUuid = UUID.randomUUID();
        order.place(placeOrderEventUuid, ORDER_NO);
        eventStore.storeAggregate(order);

        order.cancel();
        eventStore.storeAggregate(order);

        List<PublishableEvent> firstPage = ((EventPagingSupport) eventStore).getEventsSince(lastEventUuid, 1);
        assertThat(firstPage).isNotEmpty();

        List<PublishableEvent> secondPage = ((EventPagingSupport) eventStore).getEventsSince(firstPage.get(firstPage.size() - 1).getUuid(), 1);
        assertThat(secondPage).isNotEmpty();
        assertThat(secondPage.stream().map(PublishableEvent::getUuid).collect(Collectors.toSet()))
            .doesNotContainAnyElementsOf(firstPage.stream().map(PublishableEvent::getUuid).collect(Collectors.toSet()));
    }

    @Test
    public void detectsUnknownLastEvent() {
        assumeThat(this.eventStore).isInstanceOf(EventPagingSupport.class);

        assertThatThrownBy(() -> {
            ((EventPagingSupport) eventStore).getEventsSince(UUID.randomUUID(), 1000);
        }).isInstanceOf(IllegalArgumentException.class);

    }
}
