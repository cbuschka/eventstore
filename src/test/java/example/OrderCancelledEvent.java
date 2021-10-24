package example;

import com.github.cbuschka.eventstore.annotations.Event;
import com.github.cbuschka.eventstore.annotations.EventId;

import java.util.UUID;

@Event
public class OrderCancelledEvent {
    @EventId
    private UUID eventUuid;

}
