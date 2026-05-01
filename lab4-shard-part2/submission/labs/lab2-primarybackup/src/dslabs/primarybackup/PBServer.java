package dslabs.primarybackup;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

// additional imports by me
import static dslabs.primarybackup.PingTimer.PING_MILLIS;
import static dslabs.primarybackup.ViewServer.STARTUP_VIEWNUM;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PBServer extends Node {
  private final Address viewServer;

  // Your code here...
  private AMOApplication<Application> app;
  private View currentView;
  private int viewNumToPing;
  private boolean stateTransferred;
  private int stateTransferViewNum;
  private AMOCommand pendingCommand;
  private Address pendingClient;

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  PBServer(Address address, Address viewServer, Application app) {
    super(address);
    this.viewServer = viewServer;

    // Your code here...
    this.app = new AMOApplication<>(app);
  }

  @Override
  public void init() {
    // Your code here...
    currentView = new View(STARTUP_VIEWNUM, null, null);
    viewNumToPing = STARTUP_VIEWNUM;
    stateTransferred = false;
    stateTransferViewNum = -1;

    send(new Ping(STARTUP_VIEWNUM), viewServer);
    set(new PingTimer(), PING_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  // my additional functions
  private void handleForwardRequest(ForwardRequest m, Address sender) {
    if (!isBackup()) return;
    if (!sender.equals(currentView.primary())) return;
    if (m.viewNum() != currentView.viewNum()) return;

    AMOResult result = app.execute(m.command());
    send(new ForwardReply(m.command(), result, currentView.viewNum()), sender);
  }
  
  private void handleForwardReply(ForwardReply m, Address sender) {
    if (!isPrimary()) return;
    if (pendingCommand == null) return;
    if (currentView.backup() == null || !sender.equals(currentView.backup())) return;
    if (m.viewNum() != currentView.viewNum()) return;
    if (!m.command().equals(pendingCommand)) return;

    AMOResult result = app.execute(pendingCommand);
    send(new Reply(result), pendingClient);
    pendingCommand = null;
    pendingClient = null;
  }

  private void handleStateTransfer(StateTransfer m, Address sender) {
    if (!isBackup()) return;
    if (!sender.equals(currentView.primary())) return;
    if (m.viewNum() != currentView.viewNum()) return;

    if (stateTransferViewNum != currentView.viewNum()) { // only apply state once per view (prevents retries from overwriting state after already processing ForwardRequests)
      this.app = m.app();
      stateTransferViewNum = currentView.viewNum();
    }

    send(new StateTransferReply(currentView.viewNum()), sender);
  }

  private void handleStateTransferReply(StateTransferReply m, Address sender) {
    if (!isPrimary()) return;
    if (currentView.backup() == null || !sender.equals(currentView.backup())) return;
    if (m.viewNum() != currentView.viewNum()) return;
    stateTransferred = true;
    viewNumToPing = currentView.viewNum();
  }
  // end of my additional functions

  private void handleRequest(Request m, Address sender) {
    // Your code here...
    if (!isPrimary()) return;

    Address backup = currentView.backup();

    if (backup != null) {
      if (!stateTransferred) return;

      if (pendingCommand != null) {
        if (pendingCommand.equals(m.command())) send(new ForwardRequest(m.command(), currentView.viewNum()), backup);
        return;
      }

      pendingCommand = m.command();
      pendingClient = sender;
      send(new ForwardRequest(m.command(), currentView.viewNum()), backup);
    } else {
      AMOResult result = app.execute(m.command());
      send(new Reply(result), sender);
    }
  }

  private void handleViewReply(ViewReply m, Address sender) {
    // Your code here...
    View newView = m.view();

    if (newView.viewNum() <= currentView.viewNum()) return;

    View oldView = currentView;
    currentView = newView;

    if (isPrimary()) {
      Address oldBackup = oldView.backup();
      Address newBackup = newView.backup();
      
      boolean backupChanged;
      if (newBackup == null) backupChanged = (oldBackup != null);
      else backupChanged = !newBackup.equals(oldBackup);

      if (backupChanged) {
        pendingCommand = null;
        pendingClient = null;

        if (newBackup != null) {
          send(new StateTransfer(app, currentView.viewNum()), newBackup);
          stateTransferred = false;
        } else {
          stateTransferred = false;
          viewNumToPing = currentView.viewNum();
        }
      } else {
        if (newBackup == null || stateTransferred) viewNumToPing = currentView.viewNum();
      }
    } else {
      pendingCommand = null;
      pendingClient = null;
      viewNumToPing = currentView.viewNum();
    }
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingTimer(PingTimer t) {
    // Your code here...
    send(new Ping(viewNumToPing), viewServer);
    if (isPrimary() && currentView.backup() != null) {
      if (!stateTransferred) send(new StateTransfer(app, currentView.viewNum()), currentView.backup());
      else if (pendingCommand != null) send(new ForwardRequest(pendingCommand, currentView.viewNum()), currentView.backup());
    }
    set(t, PING_MILLIS);
  }

  // Your code here...

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private boolean isPrimary() {
    return currentView.primary() != null && address().equals(currentView.primary());
  }

  private boolean isBackup() {
    return currentView.backup() != null && address().equals(currentView.backup());
  }
}
