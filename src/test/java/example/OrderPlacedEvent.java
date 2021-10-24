package example;

import com.github.cbuschka.eventstore.annotations.Event;
import com.github.cbuschka.eventstore.annotations.EventId;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Event
@AllArgsConstructor
@NoArgsConstructor
public class OrderPlacedEvent {
    @EventId
    private UUID eventUuid;

    public UUID placeEventUuid;

    public String orderNo;
}
