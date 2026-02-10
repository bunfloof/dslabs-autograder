package dslabs.atmostonce;

import dslabs.framework.Command;
import lombok.Data;

// additional imports by me
import dslabs.framework.Address;

@Data
public final class AMOCommand implements Command {
  // Your code here...
  private final Command command;
  private final Address clientId;
  private final int sequenceNum;
}
