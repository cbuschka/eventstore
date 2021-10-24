package example;

import com.github.cbuschka.eventstore.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Aggregate(snapshotsEnabled = true)
public class Order {

    @AggregateId
    private UUID aggregateUuid;
    @AggregateVersion
    private int version = 0;
    @EventList
    private List<Object> events = new ArrayList<>();
    @AggregateCorrelationId
    private UUID placeOrderEventUuid;

    private String orderNo;
    private OrderState state = OrderState.NEW;

    public String getOrderNo() {
        return orderNo;
    }

    public OrderState getState() {
        return state;
    }

    @EventHandler
    private void apply(OrderPlacedEvent ev) {
        this.orderNo = ev.orderNo;
        this.state = OrderState.PLACED;
        this.placeOrderEventUuid = ev.placeEventUuid;
        this.events.add(ev);
    }

    @EventHandler
    private void apply(OrderCancelledEvent ev) {
        this.state = OrderState.CANCELLED;
        this.events.add(ev);
    }

    public void place(UUID placeOrderEventUuid, String orderNo) {
        OrderPlacedEvent event = new OrderPlacedEvent(null, placeOrderEventUuid, orderNo);
        apply(event);
    }

    public void cancel() {
        if (this.state != OrderState.PLACED) {
            throw new IllegalStateException("Cannot cancel order in state " + this.state);
        }

        OrderCancelledEvent ev = new OrderCancelledEvent();
        apply(ev);
    }
}
