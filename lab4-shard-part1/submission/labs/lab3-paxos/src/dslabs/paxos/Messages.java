package dslabs.paxos;

// Your code here...
import dslabs.atmostonce.AMOCommand;
import dslabs.framework.Address;
import dslabs.framework.Message;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;

@Data
final class Ballot implements Serializable, Comparable<Ballot> {
    private final int round;
    private final Address address;

    @Override
    public int compareTo(Ballot o) {
        int c = Integer.compare(this.round, o.round);
        return c != 0 ? c : this.address.compareTo(o.address);
    }
}

@Data
final class Pvalue implements Serializable {
    private final Ballot ballot;
    private final AMOCommand command;
    private final boolean chosen;
}

@Data
final class P1a implements Message {
    private final Ballot ballot;
}

@Data
final class P1b implements Message {
    private final Ballot ballot;
    private final Map<Integer, Pvalue> accepted;
    private final int firstNonCleared;
}

@Data
final class P2a implements Message {
    private final Ballot ballot;
    private final int slot;
    private final AMOCommand command;
}

@Data
final class P2b implements Message {
    private final Ballot ballot;
    private final int slot;
}

@Data
final class Heartbeat implements Message {
    private final Ballot ballot;
    private final int slotCleared;
}

@Data
final class HeartbeatReply implements Message {
    private final int slotExecuted;
}

@Data
final class Decision implements Message {
    private final int slot;
    private final AMOCommand command;
}
