package dslabs.clientserver;

import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Node;
import dslabs.framework.Result;
import lombok.EqualsAndHashCode;
import lombok.ToString;

// additional imports by me
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

/**
 * Simple client that sends requests to a single server and returns responses.
 *
 * <p>See the documentation of {@link Client} and {@link Node} for important implementation notes.
 */
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class SimpleClient extends Node implements Client {
  private final Address serverAddress;

  // Your code here...
  // Java likes to be verbose with private
  private int sequenceNum; // pretend that this is a signed int32 that starts at 0 by default
  private AMOCommand amoCommand; // pretend this is Option<Command> but Java lets this blow up with NullPointerException 
  private Result result; // pretend this is Option<Result> but Java lets this blow up with NullPointerException

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public SimpleClient(Address address, Address serverAddress) {
    super(address);
    this.serverAddress = serverAddress;
  }

  @Override
  public synchronized void init() {
    // No initialization necessary
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    sequenceNum++; // new request, so increment sequence number
    amoCommand = new AMOCommand(command, this.address(), sequenceNum); // store the command, this.command is not local to sendCommand
    result = null; // no answer yet

    send(new Request(amoCommand), serverAddress); // send the request to the server
    set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS); // set 100ms timer, if no reply, then retry
  }

  @Override
  public synchronized boolean hasResult() {
    // Your code here...
    if (this.result != null) return true;
    return false;
  }

  @Override
  public synchronized Result getResult() throws InterruptedException {
    // Your code here...
    while (result == null) wait(); // if we don't have an response yet, sleep until handleReply wakes us up with notify()
    return result;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    if (m.result().sequenceNum() == sequenceNum) { // only accept this response if it's for the current request
      result = m.result().result(); // store the result unwrap
      notify(); // wake up getResult() which is waiting on this.result
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (t.sequenceNum() == sequenceNum && result == null) { // only retry if this timer is for our current request and we still don't have a response. If already got the response, then do nothing (Resend/Discard Pattern).
      send(new Request(amoCommand), serverAddress); // resend request to server
      set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS); // set 100ms timer, if no reply, then retry
    }
  }
}
