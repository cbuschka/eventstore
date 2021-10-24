package example_with_correlation;

import com.github.cbuschka.eventstore.annotations.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Aggregate(snapshotsEnabled = true)
public class Contract {

    @AggregateId
    private UUID aggregateUuid;
    @AggregateVersion
    private int version = 0;
    @EventList
    private List<Object> events = new ArrayList<>();

    private String contractNo;
    private ContractState state = ContractState.NEW;

    public String getContractNo() {
        return contractNo;
    }

    public ContractState getState() {
        return state;
    }

    @EventHandler
    private void apply(ContractSignedEvent ev) {
        this.contractNo = ev.orderNo;
        this.state = ContractState.SIGNED;
        this.events.add(ev);
    }

    @EventHandler
    private void apply(ContractCancelledEvent ev) {
        this.state = ContractState.CANCELLED;
        this.events.add(ev);
    }

    public void sign(String contractNo) {
        ContractSignedEvent event = new ContractSignedEvent(null, contractNo);
        apply(event);
    }

    public void cancel() {
        if (this.state != ContractState.SIGNED) {
            throw new IllegalStateException("Cannot cancel order in state " + this.state);
        }

        ContractCancelledEvent ev = new ContractCancelledEvent();
        apply(ev);
    }
}
