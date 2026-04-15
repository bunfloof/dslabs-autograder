package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Client;
import dslabs.framework.Command;
import dslabs.framework.Result;
import dslabs.kvstore.KVStore.SingleKeyCommand;
import dslabs.kvstore.TransactionalKVStore.Transaction;
import dslabs.paxos.PaxosReply;
import dslabs.paxos.PaxosRequest;
import dslabs.shardmaster.ShardMaster.Query;
import dslabs.shardmaster.ShardMaster.ShardConfig;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ShardStoreClient extends ShardStoreNode implements Client {
  // Your code here...
  private int sequenceNum;
  private AMOCommand currentCommand;
  private Result result;
  private ShardConfig currentConfig;
  private Integer pinnedTxnGroupId;
  private int querySeqNum = 0;
  private static final int SINGLE_KEY_RETRY_MILLIS = 80;
  /* -----------------------------------------------------------------------------------------------
   *  Construction and Initialization
   * ---------------------------------------------------------------------------------------------*/
  public ShardStoreClient(Address address, Address[] shardMasters, int numShards) {
    super(address, shardMasters, numShards);
  }

  @Override
  public synchronized void init() {
    // Your code here...
    sendQuery();
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
    pinnedTxnGroupId = null;
    sendCurrentCommand();
    set(new ClientTimer(currentCommand), retryMillisForCurrentCommand());
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
  private synchronized void handleShardStoreReply(ShardStoreReply m, Address sender) {
    // Your code here...
    if (m.result() != null && m.result().sequenceNum() == sequenceNum) {
      result = m.result().result();
      notify();
    }
  }

  // Your code here...
  private synchronized void handleShardStoreError(ShardStoreError m, Address sender) {
    if (m.command() != null && m.command().equals(currentCommand) && result == null) {
      pinnedTxnGroupId = null;
      sendQuery();
    }
  }

  private synchronized void handlePaxosReply(PaxosReply m, Address sender) {
    if (m.result() != null && m.result().result() instanceof ShardConfig) {
      ShardConfig config = (ShardConfig) m.result().result();
      if (currentConfig == null || config.configNum() > currentConfig.configNum()) {
        currentConfig = config;
        if (currentCommand != null && result == null) sendCurrentCommand();
      }
    }
  }

  /* -----------------------------------------------------------------------------------------------
   *  Timer Handlers
   * ---------------------------------------------------------------------------------------------*/
  private synchronized void onClientTimer(ClientTimer t) {
    // Your code here...
    if (t.command().equals(currentCommand) && result == null) {
      sendQuery();
      sendCurrentCommand();
      set(new ClientTimer(currentCommand), retryMillisForCurrentCommand());
    }
  }

  private void sendQuery() {
    querySeqNum++;
    broadcastToShardMasters(new PaxosRequest(new AMOCommand(new Query(-1), address(), querySeqNum)));
  }

  private void sendCurrentCommand() {
    if (currentCommand == null || currentConfig == null) return;
    Command cmd = currentCommand.command();

    if (cmd instanceof SingleKeyCommand) {
      String key = ((SingleKeyCommand) cmd).key();
      int shard = keyToShard(key);
      Address[] servers = findGroupForShard(shard);
      if (servers != null) broadcast(new ShardStoreRequest(currentCommand), servers);
    } else if (cmd instanceof Transaction) {
      Transaction txn = (Transaction) cmd;

      if (pinnedTxnGroupId != null) {
        Pair<Set<Address>, Set<Integer>> pinnedInfo = currentConfig.groupInfo().get(pinnedTxnGroupId);
        if (pinnedInfo != null) {
          Address[] pinnedGroup = pinnedInfo.getLeft().toArray(new Address[0]);
          if (pinnedGroup.length > 0) {
            broadcast(new ShardStoreRequest(currentCommand), pinnedGroup);
            return;
          }
        }
        pinnedTxnGroupId = null;
      }

      int bestGroupId = -1;
      Address[] bestGroup = null;
      for (String key : txn.keySet()) {
        int shard = keyToShard(key);
        for (var entry : currentConfig.groupInfo().entrySet()) {
          if (entry.getValue().getRight().contains(shard)) {
            if (entry.getKey() > bestGroupId) {
              bestGroupId = entry.getKey();
              bestGroup = entry.getValue().getLeft().toArray(new Address[0]);
            }
          }
        }
      }
      if (bestGroup != null) {
        pinnedTxnGroupId = bestGroupId;
        broadcast(new ShardStoreRequest(currentCommand), bestGroup);
      }
    }
  }

  private Address[] findGroupForShard(int shard) {
    if (currentConfig == null) return null;
    for (var entry : currentConfig.groupInfo().entrySet()) if (entry.getValue().getRight().contains(shard)) return entry.getValue().getLeft().toArray(new Address[0]);
    return null;
  }

  private int retryMillisForCurrentCommand() {
    if (currentCommand == null) return ClientTimer.CLIENT_RETRY_MILLIS;
    return (currentCommand.command() instanceof SingleKeyCommand)
        ? SINGLE_KEY_RETRY_MILLIS
        : ClientTimer.CLIENT_RETRY_MILLIS;
  }
}
