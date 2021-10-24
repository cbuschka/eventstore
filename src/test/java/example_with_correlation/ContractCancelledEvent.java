package example_with_correlation;

import com.github.cbuschka.eventstore.annotations.Event;
import com.github.cbuschka.eventstore.annotations.EventId;

import java.util.UUID;

@Event
public class ContractCancelledEvent {
    @EventId
    private UUID eventUuid;

}
