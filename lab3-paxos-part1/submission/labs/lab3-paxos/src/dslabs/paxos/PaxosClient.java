package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public final class PaxosClient extends Node implements Client {
  private final Address[] servers;

  // Your code here...

  private int sequenceNum;
  private AMOCommand currentCommand;
  private Result result;
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosClient(Address address, Address[] servers) {
    super(address);
    this.servers = servers;
  }

  @Override
  public synchronized void init() {
    // No need to initialize
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command operation) {
    // Your code here...
    sequenceNum++;
    currentCommand = new AMOCommand(operation, address(), sequenceNum);
    result = null;

    broadcast(new PaxosRequest(currentCommand), servers);
    set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    return result != null;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (result == null) {
        wait();
    }
    return result;
  }

  /* -----------------------------------------------------------------------------------------------
   * Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    // Your code here...
    if (m.result().sequenceNum() == sequenceNum) {
        result = m.result().result();
        notify();
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (t.sequenceNum() == sequenceNum && result == null) {
        broadcast(new PaxosRequest(currentCommand), servers);
        set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
