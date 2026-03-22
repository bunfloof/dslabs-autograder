package dslabs.primarybackup;

import static dslabs.primarybackup.PingCheckTimer.PING_CHECK_MILLIS;

import dslabs.framework.Address;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;

// additional imports by me
import java.util.HashSet;
import java.util.Set;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class ViewServer extends Node {
  static final int STARTUP_VIEWNUM = 0;
  private static final int INITIAL_VIEWNUM = 1;

  // Your code here...
  private View currentView; // current view (viewNum, primary, backup)

  private boolean viewAcknowledged; // has primary acknowledged (pinged back with) the current view number?

  private Set<Address> pingSinceLastTimer; // which servers have pinged since the last PintCheckTimer fired. If a server is not in this set when timer fires, it's dead

  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ViewServer(Address address) {
    super(address);
  }

  @Override
  public void init() {
    set(new PingCheckTimer(), PING_CHECK_MILLIS);
    // Your code here...
    currentView = new View(STARTUP_VIEWNUM, null, null);
    viewAcknowledged = false;
    pingSinceLastTimer = new HashSet<>();
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePing(Ping m, Address sender) {
    // Your code here...
    // 1. record that this server is alive
    pingSinceLastTimer.add(sender);

    // 2. if there is no primary set, make the first server that pings primary
    if (currentView.viewNum() == STARTUP_VIEWNUM) {
      currentView = new View(INITIAL_VIEWNUM, sender, null);
      viewAcknowledged = false;
    }

    // 3. if primary is pinging with current view number, acknowledge it
    if (sender.equals(currentView.primary()) && m.viewNum() == currentView.viewNum()) viewAcknowledged = true;

    // 4. if view is acknowledged and there's no backup, assign an idle server as backup
    if (viewAcknowledged && currentView.backup() == null) {
      Address idle = getIdleServer(currentView.primary());
      if (idle != null) {
        currentView = new View(currentView.viewNum() + 1, currentView.primary(), idle);
        viewAcknowledged = false;
      }
    }

    // 5. always reply with current view
    send(new ViewReply(currentView), sender);
  }

  private void handleGetView(GetView m, Address sender) {
    // Your code here...
    send(new ViewReply(currentView), sender);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void onPingCheckTimer(PingCheckTimer t) {
    // Your code here...
    // only make changes if current view has been acknowledged by primary
    if (viewAcknowledged) {
      Address primary = currentView.primary();
      Address backup = currentView.backup();
      
      boolean primaryDead = primary != null &&!pingSinceLastTimer.contains(primary);
      boolean backupDead = backup != null &&!pingSinceLastTimer.contains(backup);
      
      if (primaryDead && backup != null) {
        // primary is dead and backup exists, promote backup to primary
        Address newBackup = getIdleServer(backup);
        currentView = new View(currentView.viewNum() + 1, backup, newBackup);
        viewAcknowledged = false;
      } else if (backupDead) {
        // backup is dead, remove it (possibly replace with an idle server)
        Address newBackup = getIdleServer(primary);
        currentView = new View(currentView.viewNum() + 1, primary, newBackup);
        viewAcknowledged = false;
      }
    }

    pingSinceLastTimer = new HashSet<>(); // clear ping tracking for next interval

    set(t, PING_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private Address getIdleServer(Address newPrimary) { // find an idle server that has pinged recently but is not primary, backup, or newPrimary. Return null if no idle server is available
    for (Address addr : pingSinceLastTimer) {
      if (addr.equals(newPrimary)) continue;
      if (currentView.primary() != null && addr.equals(currentView.primary())) continue;
      if (currentView.backup() != null && addr.equals(currentView.backup())) continue;
      return addr;
    }
    return null;
  }
}
