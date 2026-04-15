package dslabs.shardkv;

import dslabs.atmostonce.AMOApplication;
import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.paxos.PaxosDecision;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.paxos.PaxosServer;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreServer extends ShardStoreNode {
  private final Address[] group;
  private final int groupId;

  // Your code here...
  private static final String PAXOS_ADDRESS_ID = "paxos";
  private Address paxosAddress;

  private AMOApplication<TransactionalKVStore> app;
  private ShardConfig currentConfig;
  private int currentConfigNum = -1;

  private Set<Integer> ownedShards = new HashSet<>();

  private Set<Integer> shardsToReceive = new HashSet<>();

  private ShardConfig pendingConfig;
  private int pendingConfigNum = -1;

  private int querySeqNum = 0;

  private int internalSeqNum = -1;

  private Map<String, AMOCommand> lockedKeys = new HashMap<>();

  private Map<AMOCommand, TxnCoordinatorState> coordinatorState = new HashMap<>();

  private Map<AMOCommand, Integer> preparedTransactions = new HashMap<>();

  private Map<Integer, java.util.List<ShardTransferRequest>> bufferedTransfers = new HashMap<>();

  private ReconfigurationCommand lastSendReconf;
  private Set<Integer> preSendOwnedShards;

  private int nextSlotOut = 1;
  private java.util.TreeMap<Integer, AMOCommand> pendingDecisions = new java.util.TreeMap<>();

  private ReconfigurationCommand delayedReconf;

  private Set<AMOCommand> committedTxns = new HashSet<>();

  private Set<AMOCommand> pendingRetries = new HashSet<>();
  private Map<AMOCommand, Long> retryProgressMs = new HashMap<>();
  private Map<AMOCommand, Integer> retryProgressPrepared = new HashMap<>();
  private Map<AMOCommand, Long> preparedSinceMs = new HashMap<>();
  private Map<AMOCommand, Integer> preparedAbortIssuedAttempt = new HashMap<>();

  private Map<String, Long> txnFirstSeenMs = new HashMap<>();
  private static final long PREPARE_STUCK_ABORT_MS = 600;
  private static final long PREPARED_LOCK_MAX_MS = 2500;
  private static final long TXN_REQUEST_THROTTLE_MS = 1000;
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  ShardStoreServer(
      Address address, Address[] shardMasters, int numShards, Address[] group, int groupId) {
    super(address, shardMasters, numShards);
    this.group = group;
    this.groupId = groupId;

    // Your code here...
  }

  @Override
  public void init() {
    // Your code here...
    paxosAddress = Address.subAddress(address(), PAXOS_ADDRESS_ID);
    Address[] paxosAddresses = new Address[group.length];
    for (int i = 0; i < paxosAddresses.length; i++) paxosAddresses[i] = Address.subAddress(group[i], PAXOS_ADDRESS_ID);
    PaxosServer paxosServer = new PaxosServer(paxosAddress, paxosAddresses, address());
    addSubNode(paxosServer);
    paxosServer.init();

    app = new AMOApplication<>(new TransactionalKVStore());

    set(new QueryTimer(), QueryTimer.QUERY_MILLIS);
  }

  /* -----------------------------------------------------------------------------------------------
   *  Message Handlers
   * ---------------------------------------------------------------------------------------------*/
  private void handleShardStoreRequest(ShardStoreRequest m, Address sender) {
    // Your code here...
    AMOCommand cmd = m.command();
    Command innerCmd = cmd.command();

    if (innerCmd instanceof SingleKeyCommand) {
      String key = ((SingleKeyCommand) innerCmd).key();
      int shard = keyToShard(key);
      if (!ownedShards.contains(shard)) {
        send(new ShardStoreError(cmd), sender);
        return;
      }
    } else if (innerCmd instanceof Transaction) {
      trackTxnSeen(cmd);
    }

    if (innerCmd instanceof SingleKeyCommand || innerCmd instanceof Transaction) {
      if (app.alreadyExecuted(cmd)) {
        AMOResult result = app.execute(cmd);
        clearTxnSeen(cmd);
        send(new ShardStoreReply(result), cmd.clientId());
        return;
      }
      AMOResult cachedResult = getAmoResult(cmd.clientId());
      if (cachedResult != null && cachedResult.sequenceNum() >= cmd.sequenceNum()) {
        clearTxnSeen(cmd);
        send(new ShardStoreReply(cachedResult), cmd.clientId());
        return;
      }
      if (innerCmd instanceof Transaction) {
        Transaction txn = (Transaction) innerCmd;
        boolean ownsAny = false;
        for (String key : txn.keySet()) {
          if (ownedShards.contains(keyToShard(key))) {
            ownsAny = true;
            break;
          }
        }
        if (!ownsAny) {
          clearTxnSeen(cmd);
          send(new ShardStoreError(cmd), sender);
          return;
        }
        TxnCoordinatorState state = coordinatorState.get(cmd);
        Long firstSeen = txnFirstSeenMs.get(txnKey(cmd));
        long ageMs = (firstSeen == null) ? 0 : (System.currentTimeMillis() - firstSeen);
        if (state != null && !state.decided && state.configNum == currentConfigNum && shardsToReceive.isEmpty() && ageMs < TXN_REQUEST_THROTTLE_MS) return;
      }
    }

    handleMessage(new PaxosRequest(cmd), paxosAddress);
  }

  // Your code here...
  private void handlePaxosDecision(PaxosDecision m, Address sender) {
    AMOCommand cmd = m.command();
    if (cmd == null) return;
    int slot = m.slot();
    if (slot < nextSlotOut) return;

    pendingDecisions.put(slot, cmd);

    while (pendingDecisions.containsKey(nextSlotOut)) {
      AMOCommand nextCmd = pendingDecisions.remove(nextSlotOut);
      processDecision(nextCmd);
      nextSlotOut++;
    }
  }

  private void processDecision(AMOCommand cmd) {
    Command innerCmd = cmd.command();

    if (innerCmd instanceof ReconfigurationCommand) handleReconfigDecision((ReconfigurationCommand) innerCmd);
    else if (innerCmd instanceof ShardDataReceived) handleShardDataDecision((ShardDataReceived) innerCmd);
    else if (innerCmd instanceof TxnPrepareCommand) handleTxnPrepareDecision((TxnPrepareCommand) innerCmd, cmd);
    else if (innerCmd instanceof TxnCommitCommand) handleTxnCommitDecision((TxnCommitCommand) innerCmd, cmd);
    else if (innerCmd instanceof TxnAbortCommand) handleTxnAbortDecision((TxnAbortCommand) innerCmd, cmd);
    else if (innerCmd instanceof SingleKeyCommand) {
      AMOResult result = app.execute(cmd);
      String key = ((SingleKeyCommand) innerCmd).key();
      int shard = keyToShard(key);
      if (ownedShards.contains(shard)) send(new ShardStoreReply(result), cmd.clientId());
      } else if (innerCmd instanceof Transaction) {
        Transaction txn = (Transaction) innerCmd;
        if (group.length > 1) startTwoPhaseCommit(cmd);
        else {
          boolean allLocal = true;
          for (String key : txn.keySet()) if (!ownedShards.contains(keyToShard(key))) { allLocal = false; break; }
          if (allLocal) send(new ShardStoreReply(app.execute(cmd)), cmd.clientId());
          else startTwoPhaseCommit(cmd);
        }
      }
  }

  private void handlePaxosReply(PaxosReply m, Address sender) {
    if (m.result() != null && m.result().result() instanceof ShardConfig) {
      ShardConfig config = (ShardConfig) m.result().result();
      if (config.configNum() > currentConfigNum && pendingConfigNum < config.configNum()) {
        int nextExpected = currentConfigNum + 1;
        if (config.configNum() == nextExpected || currentConfigNum == -1) {
          internalSeqNum--;
          AMOCommand reconfCmd = new AMOCommand(new ReconfigurationCommand(config.configNum(), config.groupInfo()), address(), internalSeqNum);
          handleMessage(new PaxosRequest(reconfCmd), paxosAddress);
        }
      }
    }
  }

  private void handleShardTransferRequest(ShardTransferRequest m, Address sender) {
    if (m.configNum() == pendingConfigNum && !shardsToReceive.isEmpty()) {
      Set<Integer> needed = new HashSet<>(m.shards());
      needed.retainAll(shardsToReceive);
      if (!needed.isEmpty()) {
        internalSeqNum--;
        AMOCommand shardCmd = new AMOCommand(new ShardDataReceived(m.configNum(), m.shards(), m.data(), m.amoState()), address(), internalSeqNum);
        handleMessage(new PaxosRequest(shardCmd), paxosAddress);
      }
    } else if (m.configNum() > currentConfigNum && m.configNum() != pendingConfigNum) bufferedTransfers.computeIfAbsent(m.configNum(), k -> new java.util.ArrayList<>()).add(m);
    send(new ShardTransferReply(m.configNum(), m.shards()), sender);
  }

  private void handleShardTransferReply(ShardTransferReply m, Address sender) {
  }

  private void handlePrepare(Prepare m, Address sender) {
    Address coordinator = m.coordinator() != null ? m.coordinator() : sender;
    if (m.configNum() != currentConfigNum || !shardsToReceive.isEmpty()) {
      send(new PrepareAbort(m.configNum(), m.command(), m.attemptNum()), coordinator);
      return;
    }
    internalSeqNum--;
    AMOCommand prepCmd = new AMOCommand(new TxnPrepareCommand(m.command(), m.configNum(), m.attemptNum(), coordinator), address(), internalSeqNum);
    handleMessage(new PaxosRequest(prepCmd), paxosAddress);
  }

  private void handlePrepareOk(PrepareOk m, Address sender) {
    TxnCoordinatorState state = coordinatorState.get(m.command());
    if (state == null || state.attemptNum != m.attemptNum() || state.decided) return;

    int senderGroupId = findGroupIdForAddr(sender);
    if (senderGroupId == -1 || state.preparedGroups.contains(senderGroupId)) return;

    state.preparedGroups.add(senderGroupId);
    state.readValues.putAll(m.readValues());
    retryProgressMs.put(m.command(), System.currentTimeMillis());
    retryProgressPrepared.put(m.command(), state.preparedGroups.size());

    if (state.preparedGroups.size() >= state.participantGroups.size()) {
      state.decided = true;

      Transaction txn = (Transaction) m.command().command();
      Map<String, String> db = new HashMap<>(state.readValues);
      Result result = txn.run(db);

      for (var entry : state.participantGroups.entrySet()) 
        if (entry.getKey() != groupId) 
          for (Address addr : entry.getValue()) send(new Commit(m.configNum(), m.command(), m.attemptNum(), db, result, address()), addr);

      internalSeqNum--;
      AMOCommand commitCmd = new AMOCommand(new TxnCommitCommand(m.command(), m.configNum(), m.attemptNum(), db, result, address()), address(), internalSeqNum);
      handleMessage(new PaxosRequest(commitCmd), paxosAddress);
    }
  }

  private void handlePrepareAbort(PrepareAbort m, Address sender) {
    TxnCoordinatorState state = coordinatorState.get(m.command());
    if (state == null || state.attemptNum != m.attemptNum() || state.decided) return;
    state.decided = true;
    state.aborted = true;

    for (var entry : state.participantGroups.entrySet()) for (Address addr : entry.getValue()) send(new Abort(m.configNum(), m.command(), m.attemptNum()), addr);

    if (state.participantGroups.containsKey(groupId)) {
      internalSeqNum--;
      AMOCommand abortCmd = new AMOCommand(new TxnAbortCommand(m.command(), m.configNum(), m.attemptNum()), address(), internalSeqNum);
      handleMessage(new PaxosRequest(abortCmd), paxosAddress);
    }
    pendingRetries.add(m.command());
  }

  private void handleCommit(Commit m, Address sender) {
    if (committedTxns.contains(m.command())) {
      if (m.coordinator() != null) send(new CommitAck(m.configNum(), m.command(), m.attemptNum()), m.coordinator());
      return;
    }

    internalSeqNum--;
    AMOCommand commitCmd = new AMOCommand(new TxnCommitCommand(m.command(), m.configNum(), m.attemptNum(), m.allValues(), m.result(), m.coordinator()), address(), internalSeqNum);
    handleMessage(new PaxosRequest(commitCmd), paxosAddress);
  }

  private void handleAbort(Abort m, Address sender) {
    Integer preparedAttempt = preparedTransactions.get(m.command());
    if (preparedAttempt == null || preparedAttempt > m.attemptNum()) return;
    internalSeqNum--;
    AMOCommand abortCmd = new AMOCommand(new TxnAbortCommand(m.command(), m.configNum(), m.attemptNum()), address(), internalSeqNum);
    handleMessage(new PaxosRequest(abortCmd), paxosAddress);
  }

  private void handleCommitAck(CommitAck m, Address sender) {
    TxnCoordinatorState state = coordinatorState.get(m.command());
    if (state == null || !state.committed || state.allAcked) return;

    int senderGroupId = findGroupIdForAddr(sender);
    if (senderGroupId != -1) {
      state.commitAckedGroups.add(senderGroupId);
      if (state.commitAckedGroups.size() >= state.participantGroups.size()) state.allAcked = true;
    }
  }

  private void handleReconfigDecision(ReconfigurationCommand reconf) {
    if (reconf.configNum() <= currentConfigNum) return;
    if (currentConfigNum >= 0 && reconf.configNum() != currentConfigNum + 1) return;

    if (!lockedKeys.isEmpty()) {
      delayedReconf = reconf;
      return;
    }

    executeReconfig(reconf);
  }

  private void executeReconfig(ReconfigurationCommand reconf) {
    if (lastSendReconf != null && lastSendReconf.configNum() < reconf.configNum()) {
      lastSendReconf = null;
      preSendOwnedShards = null;
    }

    pendingConfigNum = reconf.configNum();
    pendingConfig = new ShardConfig(reconf.configNum(), reconf.groupInfo());

    Set<Integer> newShards = new HashSet<>();
    Pair<Set<Address>, Set<Integer>> myInfo = reconf.groupInfo().get(groupId);
    if (myInfo != null) newShards.addAll(myInfo.getRight());

    shardsToReceive = new HashSet<>();
    if (currentConfigNum >= 0) for (int s : newShards) if (!ownedShards.contains(s)) shardsToReceive.add(s);

    Set<Integer> shardsToSend = new HashSet<>();
    for (int s : ownedShards) if (!newShards.contains(s)) shardsToSend.add(s);

    if (!shardsToSend.isEmpty()) {
      lastSendReconf = reconf;
      preSendOwnedShards = new HashSet<>(ownedShards);

      Map<Integer, Set<Integer>> destShards = new HashMap<>();
      for (int s : shardsToSend) 
        for (var entry : reconf.groupInfo().entrySet()) 
          if (entry.getValue().getRight().contains(s)) destShards.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(s);
      for (var entry : destShards.entrySet()) sendShardData(entry.getKey(), entry.getValue(), reconf);
    }

    if (!shardsToReceive.isEmpty()) {
      java.util.List<ShardTransferRequest> buffered = bufferedTransfers.remove(reconf.configNum());
      if (buffered != null) {
        for (ShardTransferRequest req : buffered) {
          Set<Integer> needed = new HashSet<>(req.shards());
          needed.retainAll(shardsToReceive);
          if (!needed.isEmpty()) {
            internalSeqNum--;
            AMOCommand shardCmd = new AMOCommand(new ShardDataReceived(req.configNum(), req.shards(), req.data(), req.amoState()), address(), internalSeqNum);
            handleMessage(new PaxosRequest(shardCmd), paxosAddress);
          }
        }
      }
    }

    if (shardsToReceive.isEmpty()) completeReconfiguration(newShards);
  }

  private void handleShardDataDecision(ShardDataReceived shardData) {
    if (shardData.configNum() != pendingConfigNum) return;

    TransactionalKVStore kvStore = app.application();
    kvStore.putAll(shardData.data());

    if (shardData.amoState() != null) {
      for (Map.Entry<Address, AMOResult> entry : shardData.amoState().entrySet()) {
        AMOResult existing = getAmoResult(entry.getKey());
        if (existing == null || entry.getValue().sequenceNum() > existing.sequenceNum()) setAmoResult(entry.getKey(), entry.getValue());
      }
    }

    shardsToReceive.removeAll(shardData.shards());

    if (shardsToReceive.isEmpty()) {
      Set<Integer> newShards = new HashSet<>();
      Pair<Set<Address>, Set<Integer>> myInfo = pendingConfig.groupInfo().get(groupId);
      if (myInfo != null) newShards.addAll(myInfo.getRight());
      completeReconfiguration(newShards);
    }
  }

  private void handleTxnPrepareDecision(TxnPrepareCommand prep, AMOCommand wrappingCmd) {
    TxnCoordinatorState localState = coordinatorState.get(prep.command());
    int localCoordGroup =
        (localState != null && localState.coordinator != null)
            ? findGroupIdForAddr(localState.coordinator)
            : -1;
    int incomingCoordGroup = (prep.coordinator() != null) ? findGroupIdForAddr(prep.coordinator()) : -1;
    if (localState != null
        && !localState.decided
        && localState.coordinator != null
        && prep.coordinator() != null
        && localCoordGroup != -1
        && incomingCoordGroup != -1
        && localCoordGroup != incomingCoordGroup) {
      coordinatorState.remove(prep.command());
      pendingRetries.remove(prep.command());
    }

    Transaction txn = (Transaction) prep.command().command();
    boolean canLock = true;
    Set<String> myKeys = new HashSet<>();
    for (String key : txn.keySet()) {
      if (ownedShards.contains(keyToShard(key))) {
        myKeys.add(key);
        AMOCommand holder = lockedKeys.get(key);
        if (holder != null && !holder.equals(prep.command())) {
          canLock = false;
          break;
        }
      }
    }

    if (prep.configNum() != currentConfigNum) {
      send(new PrepareAbort(prep.configNum(), prep.command(), prep.attemptNum()), prep.coordinator());
      return;
    }
    if (!canLock) {
      send(new PrepareAbort(prep.configNum(), prep.command(), prep.attemptNum()), prep.coordinator());
      return;
    }

    Integer existingAttempt = preparedTransactions.get(prep.command());
    if (existingAttempt != null && existingAttempt > prep.attemptNum()) {
      send(new PrepareAbort(prep.configNum(), prep.command(), prep.attemptNum()), prep.coordinator());
      return;
    }

    boolean firstPrepareForAttempt = existingAttempt == null || existingAttempt < prep.attemptNum();
    if (firstPrepareForAttempt) {
      for (String key : myKeys) lockedKeys.put(key, prep.command());
      preparedTransactions.put(prep.command(), prep.attemptNum());
      preparedSinceMs.put(prep.command(), System.currentTimeMillis());
      preparedAbortIssuedAttempt.remove(prep.command());
    }

    Map<String, String> readValues = new HashMap<>();
    TransactionalKVStore kvStore = app.application();
    for (String key : myKeys) if (kvStore.containsKey(key)) readValues.put(key, kvStore.getValue(key));

    PrepareOk prepareOk = new PrepareOk(prep.configNum(), prep.command(), prep.attemptNum(), readValues);
    if (prep.coordinator().equals(address())) handlePrepareOk(prepareOk, address());
    else send(prepareOk, prep.coordinator());


    if (prep.coordinator().equals(address())) {
      TxnCoordinatorState cState = coordinatorState.get(prep.command());
      if (cState != null && cState.attemptNum == prep.attemptNum() && !cState.decided && !cState.remotePreparesSent) {
        cState.remotePreparesSent = true;
        for (var entry : cState.participantGroups.entrySet()) 
          if (entry.getKey() != groupId) 
            for (Address addr : entry.getValue()) send(new Prepare(prep.configNum(), prep.command(), prep.attemptNum(), prep.coordinator()), addr);
      }
    }
  }

  private void handleTxnCommitDecision(TxnCommitCommand commit, AMOCommand wrappingCmd) {
    Transaction txn = (Transaction) commit.command().command();
    TransactionalKVStore kvStore = app.application();

    if (!committedTxns.contains(commit.command())) {
      committedTxns.add(commit.command());
      for (String key : txn.writeSet())
        if (ownedShards.contains(keyToShard(key))) 
          if (commit.allValues().containsKey(key)) 
            kvStore.putAll(Map.of(key, commit.allValues().get(key))); 
            else kvStore.removeKey(key);
    }

    AMOResult amoResult = new AMOResult(commit.result(), commit.command().sequenceNum());
    AMOResult existingResult = getAmoResult(commit.command().clientId());
    if (existingResult == null || commit.command().sequenceNum() >= existingResult.sequenceNum()) setAmoResult(commit.command().clientId(), amoResult);
    clearTxnSeen(commit.command());
    send(new ShardStoreReply(amoResult), commit.command().clientId());

    for (String key : txn.keySet()) if (lockedKeys.containsKey(key) && lockedKeys.get(key).equals(commit.command())) lockedKeys.remove(key);
    preparedTransactions.remove(commit.command());
    preparedSinceMs.remove(commit.command());
    preparedAbortIssuedAttempt.remove(commit.command());
    pendingRetries.remove(commit.command());
    retryProgressMs.remove(commit.command());
    retryProgressPrepared.remove(commit.command());

    TxnCoordinatorState cState = coordinatorState.get(commit.command());
    if (cState != null) {
      cState.decided = true;
      cState.committed = true;
      cState.aborted = false;
      cState.commitValues = commit.allValues();
      cState.commitResult = commit.result();
      if (cState.participantGroups.containsKey(groupId)) cState.commitAckedGroups.add(groupId);
      if (cState.commitAckedGroups.size() >= cState.participantGroups.size()) cState.allAcked = true;
    }

    Address clientId = commit.command().clientId();
    int seqNum = commit.command().sequenceNum();
    coordinatorState.entrySet().removeIf(entry -> {
      AMOCommand cmd = entry.getKey();
      TxnCoordinatorState s = entry.getValue();
      return cmd.clientId().equals(clientId) && cmd.sequenceNum() < seqNum && s.allAcked;
    });

    if (commit.coordinator() != null) send(new CommitAck(commit.configNum(), commit.command(), commit.attemptNum()), commit.coordinator());

    checkDelayedReconfig();
  }

  private void handleTxnAbortDecision(TxnAbortCommand abort, AMOCommand wrappingCmd) {
    Integer preparedAttempt = preparedTransactions.get(abort.command());
    if (preparedAttempt == null || preparedAttempt > abort.attemptNum()) return;

    Transaction txn = (Transaction) abort.command().command();
    for (String key : txn.keySet()) if (lockedKeys.containsKey(key) && lockedKeys.get(key).equals(abort.command())) lockedKeys.remove(key);

    preparedTransactions.remove(abort.command());
    preparedSinceMs.remove(abort.command());
    preparedAbortIssuedAttempt.remove(abort.command());
    retryProgressMs.remove(abort.command());
    retryProgressPrepared.remove(abort.command());
    checkDelayedReconfig();
  }

  private void startTwoPhaseCommit(AMOCommand cmd) {
    if (app.alreadyExecuted(cmd)) {
      AMOResult result = app.execute(cmd);
      send(new ShardStoreReply(result), cmd.clientId());
      return;
    }

    AMOResult cachedResult = getAmoResult(cmd.clientId());
    if (cachedResult != null && cachedResult.sequenceNum() >= cmd.sequenceNum()) {
      send(new ShardStoreReply(cachedResult), cmd.clientId());
      return;
    }

    TxnCoordinatorState existing = coordinatorState.get(cmd);
    if (existing != null && !existing.decided) {
      if (existing.participantGroups != null && !existing.participantGroups.isEmpty()) return;
      existing.decided = true;
    }

    if (currentConfig == null || currentConfigNum < 0) {
      clearTxnSeen(cmd);
      send(new ShardStoreError(cmd), cmd.clientId());
      return;
    }

    Transaction txn = (Transaction) cmd.command();
    int attemptNum = existing != null ? existing.attemptNum + 1 : 1;

    Map<Integer, Set<Address>> participants = new HashMap<>();
    for (String key : txn.keySet()) {
      int shard = keyToShard(key);
      for (var entry : currentConfig.groupInfo().entrySet()) if (entry.getValue().getRight().contains(shard)) participants.putIfAbsent(entry.getKey(), entry.getValue().getLeft());
    }

    if (participants.isEmpty()) {
      clearTxnSeen(cmd);
      send(new ShardStoreError(cmd), cmd.clientId());
      return;
    }

    TxnCoordinatorState state = new TxnCoordinatorState();
    state.attemptNum = attemptNum;
    state.configNum = currentConfigNum;
    state.participantGroups = participants;
    state.decided = false;
    state.cmd = cmd;
    state.remotePreparesSent = false;
    state.coordinator = address();
    coordinatorState.put(cmd, state);
    long now = System.currentTimeMillis();
    retryProgressMs.put(cmd, now);
    retryProgressPrepared.put(cmd, 0);

    if (participants.containsKey(groupId)) {
      internalSeqNum--;
      AMOCommand prepCmd = new AMOCommand(new TxnPrepareCommand(cmd, currentConfigNum, attemptNum, address()),address(), internalSeqNum);
      handleMessage(new PaxosRequest(prepCmd), paxosAddress);
    } else {
      state.remotePreparesSent = true;
      for (var entry : participants.entrySet())
        for (Address addr : entry.getValue())
          send(new Prepare(currentConfigNum, cmd, attemptNum, state.coordinator), addr);
    }
  }

  private void sendShardData(int destGroupId, Set<Integer> shards, ReconfigurationCommand reconf) {
    TransactionalKVStore kvStore = app.application();
    Map<String, String> data = new HashMap<>();
    for (Map.Entry<String, String> entry : kvStore.getAllData().entrySet()) {
      int shard = keyToShard(entry.getKey());
      if (shards.contains(shard)) data.put(entry.getKey(), entry.getValue());
    }

    Map<Address, AMOResult> amoState = getAmoState();

    Pair<Set<Address>, Set<Integer>> destInfo = reconf.groupInfo().get(destGroupId);
    if (destInfo != null) for (Address addr : destInfo.getLeft()) send(new ShardTransferRequest(reconf.configNum(), shards, data, amoState), addr);
  }

  private void completeReconfiguration(Set<Integer> newShards) {
    ownedShards = newShards;
    currentConfig = pendingConfig;
    currentConfigNum = pendingConfigNum;
    pendingConfig = null;
    pendingConfigNum = -1;
    shardsToReceive = new HashSet<>();
  }

  private void checkDelayedReconfig() {
    if (delayedReconf != null && lockedKeys.isEmpty()) {
      ReconfigurationCommand reconf = delayedReconf;
      delayedReconf = null;
      executeReconfig(reconf);
    }
  }
  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private void onQueryTimer(QueryTimer t) {
    int queryNum = currentConfigNum + 1;
    if (pendingConfigNum > currentConfigNum) queryNum = pendingConfigNum + 1;
    querySeqNum++;
    broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(queryNum), address(), querySeqNum)));
    set(new QueryTimer(), QueryTimer.QUERY_MILLIS);

    if (!lockedKeys.isEmpty()) {
      java.util.Iterator<Map.Entry<String, AMOCommand>> it = lockedKeys.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, AMOCommand> e = it.next();
        if (!preparedTransactions.containsKey(e.getValue())) it.remove();
      }
    }

    long nowForPrepared = System.currentTimeMillis();
    for (var entry : new HashMap<>(preparedTransactions).entrySet()) {
      AMOCommand preparedCmd = entry.getKey();
      int preparedAttempt = entry.getValue();
      Long since = preparedSinceMs.get(preparedCmd);
      if (since == null || nowForPrepared - since < PREPARED_LOCK_MAX_MS) continue;
      Integer issuedAttempt = preparedAbortIssuedAttempt.get(preparedCmd);
      if (issuedAttempt != null && issuedAttempt >= preparedAttempt) continue;
      internalSeqNum--;
      AMOCommand abortCmd =new AMOCommand(new TxnAbortCommand(preparedCmd, currentConfigNum, preparedAttempt), address(), internalSeqNum);
      handleMessage(new PaxosRequest(abortCmd), paxosAddress);
      preparedAbortIssuedAttempt.put(preparedCmd, preparedAttempt);
    }

    if (pendingConfig != null && pendingConfigNum > currentConfigNum) retryShardTransfers();

    if (lastSendReconf != null && preSendOwnedShards != null) retryOutgoingSends();

    if (!pendingRetries.isEmpty()) {
      Set<AMOCommand> toRetry = new HashSet<>(pendingRetries);
      pendingRetries.clear();
      for (AMOCommand cmd : toRetry) {
        TxnCoordinatorState st = coordinatorState.get(cmd);
        if (st != null && st.decided && st.aborted) {
          for (var pEntry : st.participantGroups.entrySet())
            for (Address addr : pEntry.getValue()) send(new Abort(st.configNum, cmd, st.attemptNum), addr);
          if (st.participantGroups.containsKey(groupId)) {
            internalSeqNum--;
            AMOCommand abortCmd = new AMOCommand(new TxnAbortCommand(cmd, st.configNum, st.attemptNum), address(), internalSeqNum);
            handleMessage(new PaxosRequest(abortCmd), paxosAddress);
          }
        }
        TxnCoordinatorState existing = coordinatorState.get(cmd);
        if (existing != null) existing.decided = true;
        startTwoPhaseCommit(cmd);
      }
    }

    for (var entry : coordinatorState.entrySet()) {
        TxnCoordinatorState state = entry.getValue();
        if (!state.decided) {
          long now = System.currentTimeMillis();
          AMOCommand cmd = entry.getKey();
          int preparedCount = state.preparedGroups.size();
          Long lastMs = retryProgressMs.get(cmd);
          Integer lastPrepared = retryProgressPrepared.get(cmd);
          if (lastMs == null || lastPrepared == null || preparedCount != lastPrepared) {
            retryProgressMs.put(cmd, now);
            retryProgressPrepared.put(cmd, preparedCount);
          } else if (preparedCount < state.participantGroups.size() && now - lastMs >= PREPARE_STUCK_ABORT_MS) {
            state.decided = true;
            state.aborted = true;
            pendingRetries.add(cmd);
            retryProgressMs.put(cmd, now);
          }
        }

        if (!state.decided && !state.remotePreparesSent && state.participantGroups.containsKey(groupId)) {
          internalSeqNum--;
          AMOCommand prepCmd = new AMOCommand(new TxnPrepareCommand(entry.getKey(), state.configNum, state.attemptNum, address()), address(), internalSeqNum);
          handleMessage(new PaxosRequest(prepCmd), paxosAddress);
        }

        if (!state.decided && state.remotePreparesSent) 
          for (var pEntry : state.participantGroups.entrySet()) 
            if (pEntry.getKey() != groupId && !state.preparedGroups.contains(pEntry.getKey())) 
              for (Address addr : pEntry.getValue()) send(new Prepare(state.configNum, entry.getKey(), state.attemptNum, state.coordinator), addr);

      if (state.committed && state.commitValues != null && !state.allAcked) {
        AMOResult latest = getAmoResult(entry.getKey().clientId());
        if (latest != null && latest.sequenceNum() >= entry.getKey().sequenceNum()) send(new ShardStoreReply(latest), entry.getKey().clientId());
          if (state.commitAckedGroups.size() >= state.participantGroups.size()) state.allAcked = true;
          else for (var pEntry : state.participantGroups.entrySet()) if (pEntry.getKey() != groupId && !state.commitAckedGroups.contains(pEntry.getKey())) for (Address addr : pEntry.getValue()) send(new Commit(state.configNum, entry.getKey(), state.attemptNum, state.commitValues, state.commitResult, address()), addr);
      }
    }
  }

  private void retryShardTransfers() {
    if (pendingConfig == null) return;
    Set<Integer> shardsToSend = new HashSet<>();
    Pair<Set<Address>, Set<Integer>> myInfo = pendingConfig.groupInfo().get(groupId);
    Set<Integer> newShards = (myInfo != null) ? myInfo.getRight() : new HashSet<>();
    for (int s : ownedShards) if (!newShards.contains(s)) shardsToSend.add(s);

    if (!shardsToSend.isEmpty()) {
      Map<Integer, Set<Integer>> destShards = new HashMap<>();
      for (int s : shardsToSend)
        for (var entry : pendingConfig.groupInfo().entrySet())
          if (entry.getValue().getRight().contains(s)) destShards.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(s);
      for (var entry : destShards.entrySet()) sendShardData(entry.getKey(), entry.getValue(), new ReconfigurationCommand(pendingConfig.configNum(), pendingConfig.groupInfo()));
    }
  }

  private void retryOutgoingSends() {
    if (lastSendReconf == null || preSendOwnedShards == null) return;
    Pair<Set<Address>, Set<Integer>> myInfo = lastSendReconf.groupInfo().get(groupId);
    Set<Integer> newShards = (myInfo != null) ? myInfo.getRight() : new HashSet<>();
    Map<Integer, Set<Integer>> destShards = new HashMap<>();
    for (int s : preSendOwnedShards)
      if (!newShards.contains(s)) 
        for (var entry : lastSendReconf.groupInfo().entrySet()) 
          if (entry.getValue().getRight().contains(s)) destShards.computeIfAbsent(entry.getKey(), k -> new HashSet<>()).add(s);
    for (var entry : destShards.entrySet()) sendShardData(entry.getKey(), entry.getValue(), lastSendReconf);
  }


  /* -----------------------------------------------------------------------------------------------
   *  Utils
   * ---------------------------------------------------------------------------------------------*/
  // Your code here...
  private int findGroupIdForAddr(Address addr) {
    if (currentConfig == null) return -1;
    for (var entry : currentConfig.groupInfo().entrySet()) if (entry.getValue().getLeft().contains(addr)) return entry.getKey();
    for (Address a : group) if (a.equals(addr)) return groupId;
    return -1;
  }

  private Address[] findGroup(Address serverAddr) {
    if (currentConfig == null) return new Address[]{serverAddr};
    for (var entry : currentConfig.groupInfo().entrySet()) if (entry.getValue().getLeft().contains(serverAddr)) return entry.getValue().getLeft().toArray(new Address[0]);
    return new Address[]{serverAddr};
  }

  @SuppressWarnings("unchecked")
  private Map<Address, AMOResult> getAmoState() {
    try {
      var field = AMOApplication.class.getDeclaredField("lastResults");
      field.setAccessible(true);
      return new HashMap<>((Map<Address, AMOResult>) field.get(app));
    } catch (Exception e) { return new HashMap<>(); }
  }

  private AMOResult getAmoResult(Address clientId) {
    return getAmoState().get(clientId);
  }

  private String txnKey(AMOCommand cmd) {
    return cmd.clientId() + "#" + cmd.sequenceNum();
  }

  private void trackTxnSeen(AMOCommand cmd) {
    String key = txnKey(cmd);
    txnFirstSeenMs.putIfAbsent(key, System.currentTimeMillis());
  }

  private void clearTxnSeen(AMOCommand cmd) {
    String key = txnKey(cmd);
    txnFirstSeenMs.remove(key);
    retryProgressMs.remove(cmd);
    retryProgressPrepared.remove(cmd);
    preparedSinceMs.remove(cmd);
    preparedAbortIssuedAttempt.remove(cmd);
  }

  @SuppressWarnings("unchecked")
  private void setAmoResult(Address clientId, AMOResult result) {
    try {
      var field = AMOApplication.class.getDeclaredField("lastResults");
      field.setAccessible(true);
      Map<Address, AMOResult> map = (Map<Address, AMOResult>) field.get(app);
      map.put(clientId, result);
    } catch (Exception e) { /* ignore */ }
  }

  static class TxnCoordinatorState implements java.io.Serializable {
    int attemptNum;
    int configNum;
    Map<Integer, Set<Address>> participantGroups;
    Set<Integer> preparedGroups = new HashSet<>();
    Map<String, String> readValues = new HashMap<>();
    boolean decided;
    AMOCommand cmd;
    Address coordinator;
    boolean remotePreparesSent;
    boolean committed;
    Map<String, String> commitValues;
    Result commitResult;
    Set<Integer> commitAckedGroups = new HashSet<>();
    boolean allAcked;
    boolean aborted;
  }
}
