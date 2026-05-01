package dslabs.primarybackup;

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

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBClient extends Node implements Client {
  private final Address viewServer;

  // Your code here...
  private View currentView;
  private AMOCommand currentCommand;
  private Result result;
  private int sequenceNum;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PBClient(Address address, Address viewServer) {
    super(address);
    this.viewServer = viewServer;
  }

  @Override
  public synchronized void init() {
    // Your code here...
    send(new GetView(), viewServer);
    set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Client Methods
   * ---------------------------------------------------------------------------------------------*/
  @Override
  public synchronized void sendCommand(Command command) {
    // Your code here...
    sequenceNum++;
    currentCommand = new AMOCommand(command, address(), sequenceNum);
    result = null;

    // send to primary if we know who it is
    if (currentView != null && currentView.primary() != null) send(new Request(currentCommand), currentView.primary());

    // set retry timer (timer will also ask ViewServer for fresh view)
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
    while (result == null) wait();
    return result;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void handleReply(Reply m, Address sender) {
    // Your code here...
    if (m.result().sequenceNum() == sequenceNum) {
      result = m.result().result();
      notify();
    }
  }

  private synchronized void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    currentView = m.view();

    // if we have a pending command and now know a primary, resend
    if (currentCommand != null && currentView.primary() != null) send(new Request(currentCommand), currentView.primary());
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    // ask ViewServer for a fresh view (maybe primary changed)
    if (t.sequenceNum() == sequenceNum && result == null) {
      send(new GetView(), viewServer);

      // resend request to current primary if we know one
      if (currentView != null && currentView.primary() != null) send(new Request(currentCommand), currentView.primary());
      
      set(new ClientTimer(sequenceNum), ClientTimer.CLIENT_RETRY_MILLIS);
    }
  }
}
