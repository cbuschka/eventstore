package example_with_correlation;

import com.github.cbuschka.eventstore.annotations.Event;
import com.github.cbuschka.eventstore.annotations.EventId;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Event
@AllArgsConstructor
@NoArgsConstructor
public class ContractSignedEvent {
    @EventId
    private UUID eventUuid;

    public String orderNo;
}
