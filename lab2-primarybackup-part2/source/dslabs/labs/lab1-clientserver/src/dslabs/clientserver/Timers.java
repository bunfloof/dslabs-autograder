package dslabs.clientserver;

import dslabs.framework.Timer;
import lombok.Data;

@Data
final class ClientTimer implements Timer {
  static final int CLIENT_RETRY_MILLIS = 100;

  // Your code here...
  private final int sequenceNum; // pretend this is a signed int32 owned and immutable
}
