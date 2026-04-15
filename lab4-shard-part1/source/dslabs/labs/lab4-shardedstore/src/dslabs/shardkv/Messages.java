package dslabs.shardkv;

import dslabs.atmostonce.AMOCommand;
import dslabs.atmostonce.AMOResult;
import dslabs.framework.Address;
import dslabs.framework.Command;
import dslabs.framework.Message;
import dslabs.framework.Result;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
final class ShardStoreRequest implements Message {
  // Your code here...
  private final AMOCommand command;
}

@Data
final class ShardStoreReply implements Message {
  // Your code here...
  private final AMOResult result;
}

// Your code here...

@Data
final class ShardStoreError implements Message {
  private final AMOCommand command;
}

@Data
final class ShardTransferRequest implements Message {
  private final int configNum;
  private final Set<Integer> shards;
  private final Map<String, String> data;
  private final Map<Address, AMOResult> amoState;
}

@Data
final class ShardTransferReply implements Message {
  private final int configNum;
  private final Set<Integer> shards;
}

@Data
final class ReconfigurationCommand implements Command, Serializable {
  private final int configNum;
  private final Map<Integer, org.apache.commons.lang3.tuple.Pair<Set<Address>, Set<Integer>>> groupInfo;
}

@Data
final class ShardDataReceived implements Command, Serializable {
  private final int configNum;
  private final Set<Integer> shards;
  private final Map<String, String> data;
  private final Map<Address, AMOResult> amoState;
}

@Data
final class Prepare implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
  private final Address coordinator;
}

@Data
final class PrepareOk implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
  private final Map<String, String> readValues;
}

@Data
final class PrepareAbort implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
}

@Data
final class Commit implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
  private final Map<String, String> allValues;
  private final Result result;
  private final Address coordinator;
}

@Data
final class CommitAck implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
}

@Data
final class Abort implements Message {
  private final int configNum;
  private final AMOCommand command;
  private final int attemptNum;
}

@Data
final class TxnPrepareCommand implements Command, Serializable {
  private final AMOCommand command;
  private final int configNum;
  private final int attemptNum;
  private final Address coordinator;
}

@Data
final class TxnCommitCommand implements Command, Serializable {
  private final AMOCommand command;
  private final int configNum;
  private final int attemptNum;
  private final Map<String, String> allValues;
  private final Result result;
  private final Address coordinator;
}

@Data
final class TxnAbortCommand implements Command, Serializable {
  private final AMOCommand command;
  private final int configNum;
  private final int attemptNum;
}
