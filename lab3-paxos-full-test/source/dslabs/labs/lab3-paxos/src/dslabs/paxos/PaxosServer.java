package dslabs.paxos;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Node;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class PaxosServer extends Node {
  /** All servers in the Paxos group, including this one. */
  private final Address[] servers;

  // Your code here...
  private AMOApplication<Application> app;

  private HashMap<Integer, AMOCommand> log;
  private HashMap<Integer, Ballot> logBallots;
  private HashSet<Integer> chosenSlots;
  private int slotOut;
  private int slotIn;
  private int gcSlot;

  private Ballot ballot;
  private boolean active;

  private HashMap<Address, P1b> p1bResponses;

  private HashMap<Integer, HashSet<Address>> p2bVoters;

  private boolean heardFromLeader;
  private int heartbeatMissCount;

  private HashMap<Address, Integer> serverSlotExecuted;
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public PaxosServer(Address address, Address[] servers, Application app) {
    super(address);
    this.servers = servers;

    // Your code here...
    this.app = new AMOApplication<>(app);
    this.log = new HashMap<>();
    this.logBallots = new HashMap<>();
    this.chosenSlots = new HashSet<>();
    this.slotOut = 1;
    this.slotIn = 1;
    this.gcSlot = 0;

    this.active = false;
    this.p1bResponses = new HashMap<>();
    this.p2bVoters = new HashMap<>();

    this.heardFromLeader = false;
    this.heartbeatMissCount = 0;
    this.serverSlotExecuted = new HashMap<>();
  }

  @Override
  public void init() {
    // Your code here...
    startPhase1();
    set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Interface Methods
   *
   *  Be sure to implement the following methods correctly. The test code uses them to check
   *  correctness more efficiently.
   * ---------------------------------------------------------------------------------------------*/

  /**
   * Return the status of a given slot in the server's local log.
   *
   * <p>If this server has garbage-collected this slot, it should return {@link
   * PaxosLogSlotStatus#CLEARED} even if it has previously accepted or chosen command for this slot.
   * If this server has both accepted and chosen a command for this slot, it should return {@link
   * PaxosLogSlotStatus#CHOSEN}.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's status
   * @see PaxosLogSlotStatus
   */
  public PaxosLogSlotStatus status(int logSlotNum) {
    // Your code here...
    if (logSlotNum <= gcSlot) return PaxosLogSlotStatus.CLEARED;
    if (chosenSlots.contains(logSlotNum)) return PaxosLogSlotStatus.CHOSEN;
    if (log.containsKey(logSlotNum)) return PaxosLogSlotStatus.ACCEPTED;
    return PaxosLogSlotStatus.EMPTY;
  }

  /**
   * Return the command associated with a given slot in the server's local log.
   *
   * <p>If the slot has status {@link PaxosLogSlotStatus#CLEARED} or {@link
   * PaxosLogSlotStatus#EMPTY}, this method should return {@code null}. Otherwise, return the
   * command this server has chosen or accepted, according to {@link PaxosServer#status}.
   *
   * <p>If clients wrapped commands in {@link dslabs.atmostonce.AMOCommand}, this method should
   * unwrap them before returning.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @param logSlotNum the index of the log slot
   * @return the slot's contents or {@code null}
   * @see PaxosLogSlotStatus
   */
  public Command command(int logSlotNum) {
    // Your code here...
    if (logSlotNum <= gcSlot) return null;
    if (!log.containsKey(logSlotNum)) return null;
    AMOCommand amoCmd = log.get(logSlotNum);
    return amoCmd != null ? amoCmd.command() : null;
  }

  /**
   * Return the index of the first non-cleared slot in the server's local log. The first non-cleared
   * slot is the first slot which has not yet been garbage-collected. By default, the first
   * non-cleared slot is 1.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int firstNonCleared() {
    // Your code here...
    return gcSlot + 1;
  }

  /**
   * Return the index of the last non-empty slot in the server's local log, according to the defined
   * states in {@link PaxosLogSlotStatus}. If there are no non-empty slots in the log, this method
   * should return 0.
   *
   * <p>Log slots are numbered starting with 1.
   *
   * @return the index in the log
   * @see PaxosLogSlotStatus
   */
  public int lastNonEmpty() {
    // Your code here...
    int max = gcSlot; // cleared slots count as non-empty
    for (int slot : log.keySet()) if (slot > max) max = slot;
    return max;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handlePaxosRequest(PaxosRequest m, Address sender) {
    // Your code here...
    AMOCommand cmd = m.command();
    
    if (app.alreadyExecuted(cmd)) {
        AMOResult result = app.execute(cmd);
        send(new PaxosReply(result), sender);
        return;
    }

    if (active) {
        int existingSlot = findSlotForCommand(cmd);
        if (existingSlot > 0) {
            if (!chosenSlots.contains(existingSlot) && existingSlot > gcSlot) {
                P2a p2a = new P2a(ballot, existingSlot, cmd);
                for (Address s : servers) if (!s.equals(address())) send(p2a, s);
            }
        } else proposeNewCommand(cmd);
    }
  }

  // Your code here...
  private void handleP1a(P1a m, Address sender) {
    if (m.ballot().compareTo(ballot) >= 0) {
        ballot = m.ballot();
        if (active && !ballot.address().equals(address())) active = false;
    }
    send(new P1b(ballot, buildLogSnapshot(), gcSlot + 1), sender);
  }

  private void handleP1b(P1b m, Address sender) {
    if (active) return;
    if (m.ballot().compareTo(ballot) > 0) {
        ballot = m.ballot();
        return;
    }
    if (!m.ballot().equals(ballot) || !ballot.address().equals(address())) return;

    p1bResponses.put(sender, m);

    if (p1bResponses.size() > servers.length / 2) becomeActive();
  }

  private void handleP2a(P2a m, Address sender) {
    if (m.ballot().compareTo(ballot) >= 0) {
        ballot = m.ballot();
        if (active && !ballot.address().equals(address())) active = false;
        if (!ballot.address().equals(address())) {
            heardFromLeader = true;
            heartbeatMissCount = 0;
        }
        int slot = m.slot();
        if (slot > gcSlot && !chosenSlots.contains(slot)) {
            log.put(slot, m.command());
            logBallots.put(slot, m.ballot());
        }
        send(new P2b(ballot, m.slot()), sender);
    }
  }

  private void handleP2b(P2b m, Address sender) {
    if (m.ballot().compareTo(ballot) > 0) {
        ballot = m.ballot();
        active = false;
        return;
    }
    if (!active || !m.ballot().equals(ballot)) return;
    int slot = m.slot();
    if (chosenSlots.contains(slot) || slot <= gcSlot) return;

    HashSet<Address> voters = p2bVoters.get(slot);
    if (voters == null) return;

    voters.add(sender);

    if (voters.size() > servers.length / 2) {
        chosenSlots.add(slot);
        executeLog();
    }
  }

  private void handleHeartbeat(Heartbeat m, Address sender) {
    if (m.ballot().compareTo(ballot) >= 0) {
        ballot = m.ballot();
        if (active && !ballot.address().equals(address())) active = false;
        heardFromLeader = true;
        heartbeatMissCount = 0;

        if (m.slotCleared() > gcSlot) garbageCollect(m.slotCleared());
    }
    send(new HeartbeatReply(slotOut - 1), sender);
  }

  private void handleHeartbeatReply(HeartbeatReply m, Address sender) {
    if (!active) return;

    serverSlotExecuted.put(sender, m.slotExecuted());

    for (int i = m.slotExecuted() + 1; i < slotOut; i++) if (i > gcSlot && chosenSlots.contains(i)) send(new Decision(i, log.get(i)), sender);

    int minExecuted = slotOut - 1;
    for (Address s : servers) {
        if (!s.equals(address())) {
            Integer exec = serverSlotExecuted.get(s);
            if (exec == null) {
                minExecuted = 0;
                break;
            }
            minExecuted = Math.min(minExecuted, exec);
        }
    }

    if (minExecuted > gcSlot) garbageCollect(minExecuted);
  }

  private void handleDecision(Decision m, Address sender) {
    int slot = m.slot();
    if (slot <= gcSlot || chosenSlots.contains(slot)) return;
    log.put(slot, m.command());
    logBallots.put(slot, ballot);
    chosenSlots.add(slot);
    executeLog();
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void onHeartbeatCheckTimer(HeartbeatCheckTimer t) {
    set(new HeartbeatCheckTimer(), HeartbeatCheckTimer.HEARTBEAT_CHECK_MILLIS);

    if (!active) {
        if (!heardFromLeader) {
            heartbeatMissCount++;
            if (heartbeatMissCount >= 2) {
                heartbeatMissCount = 0;
                startPhase1();
            }
        } else {
            heartbeatMissCount = 0;
            heardFromLeader = false;
        }
    }
  }

  private void onHeartbeatTimer(HeartbeatTimer t) {
    if (active) {
        Heartbeat hb = new Heartbeat(ballot, gcSlot);
        for (Address s : servers) if (!s.equals(address())) send(hb, s);

        for (int i = slotOut; i < slotIn; i++) {
            if (i > gcSlot && !chosenSlots.contains(i) && p2bVoters.containsKey(i)) {
                AMOCommand cmd = log.get(i);
                P2a p2a = new P2a(ballot, i, cmd);
                for (Address s : servers) if (!s.equals(address())) send(p2a, s);
            }
        }
        set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);
    }
  }

  private void startPhase1() {
    ballot = new Ballot(ballot != null ? ballot.round() + 1 : 1, address());
    active = false;
    p1bResponses = new HashMap<>();
    p2bVoters = new HashMap<>();

    P1b myP1b = new P1b(ballot, buildLogSnapshot(), gcSlot + 1);
    p1bResponses.put(address(), myP1b);

    P1a p1a = new P1a(ballot);
    for (Address s : servers) if (!s.equals(address())) send(p1a, s);

    if (p1bResponses.size() > servers.length / 2) becomeActive();
  }

  private void becomeActive() {
    active = true;
    heartbeatMissCount = 0;

    HashMap<Integer, Pvalue> merged = new HashMap<>();
    int maxSlot = slotIn - 1;

    for (P1b p1b : p1bResponses.values()) {
        if (p1b.accepted() != null) {
            for (Map.Entry<Integer, Pvalue> entry : p1b.accepted().entrySet()) {
                int slot = entry.getKey();
                Pvalue pv = entry.getValue();
                if (slot <= gcSlot) continue;
                maxSlot = Math.max(maxSlot, slot);

                Pvalue existing = merged.get(slot);
                if (existing == null) merged.put(slot, pv);
                else if (pv.chosen()) merged.put(slot, pv);
                else if (!existing.chosen() && pv.ballot().compareTo(existing.ballot()) > 0) merged.put(slot, pv);
            }
        }
    }

    slotIn = Math.max(slotIn, maxSlot + 1);

    for (int i = slotOut; i < slotIn; i++) {
        if (i <= gcSlot || chosenSlots.contains(i)) continue;

        Pvalue pv = merged.get(i);
        if (pv != null && pv.chosen()) {
            log.put(i, pv.command());
            logBallots.put(i, pv.ballot() != null ? pv.ballot() : ballot);
            chosenSlots.add(i);
        } else if (pv != null) startPhase2(i, pv.command());
        else startPhase2(i, null);
    }

    executeLog();

    serverSlotExecuted = new HashMap<>();

    set(new HeartbeatTimer(), HeartbeatTimer.HEARTBEAT_MILLIS);

    p1bResponses = new HashMap<>();
  }

  private void proposeNewCommand(AMOCommand cmd) {
    int slot = slotIn;
    slotIn++;
    startPhase2(slot, cmd);
  }

  private void startPhase2(int slot, AMOCommand cmd) {
    log.put(slot, cmd);
    logBallots.put(slot, ballot);

    HashSet<Address> voters = new HashSet<>();
    voters.add(address());
    p2bVoters.put(slot, voters);

    P2a p2a = new P2a(ballot, slot, cmd);
    for (Address s : servers) if (!s.equals(address())) send(p2a, s);

    if (voters.size() > servers.length / 2) {
        chosenSlots.add(slot);
        executeLog();
    }
  }

  private void executeLog() {
    while (chosenSlots.contains(slotOut)) {
        AMOCommand cmd = log.get(slotOut);
        if (cmd != null) {
            AMOResult result = app.execute(cmd);
            send(new PaxosReply(result), cmd.clientId());
        }
        slotOut++;
    }
  }

  private Map<Integer, Pvalue> buildLogSnapshot() {
    HashMap<Integer, Pvalue> snapshot = new HashMap<>();
    for (Map.Entry<Integer, AMOCommand> entry : log.entrySet()) {
        int slot = entry.getKey();
        if (slot <= gcSlot) continue;
        Ballot b = logBallots.get(slot);
        boolean isChosen = chosenSlots.contains(slot);
        snapshot.put(slot, new Pvalue(b, entry.getValue(), isChosen));
    }
    return snapshot;
  }

  private void garbageCollect(int upTo) {
    for (int i = gcSlot + 1; i <= upTo; i++) {
        log.remove(i);
        logBallots.remove(i);
        chosenSlots.remove(i);
        p2bVoters.remove(i);
    }
    gcSlot = upTo;
  }
  
  private int findSlotForCommand(AMOCommand cmd) {
    for (Map.Entry<Integer, AMOCommand> entry : log.entrySet()) if (cmd.equals(entry.getValue())) return entry.getKey();
    return -1;
  }

  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
}
