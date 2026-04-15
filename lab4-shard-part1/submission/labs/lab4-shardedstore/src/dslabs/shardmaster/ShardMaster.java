package dslabs.shardmaster;

import dslabs.framework.Address;
import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

@ToString
@EqualsAndHashCode
public final class ShardMaster implements Application {
  public static final int INITIAL_CONFIG_NUM = 0;

  private final int numShards;

  // Your code here...

  private final List<ShardConfig> configs = new ArrayList<>();
  private final TreeMap<Integer, Set<Address>> groups = new TreeMap<>();
  private final Map<Integer, Integer> shardToGroup = new HashMap<>();

  public ShardMaster(int numShards) { 
    this.numShards = numShards;
  }

  public interface ShardMasterCommand extends Command {}

  @Data
  public static final class Join implements ShardMasterCommand {
    private final int groupId;
    private final Set<Address> servers;
  }

  @Data
  public static final class Leave implements ShardMasterCommand {
    private final int groupId;
  }

  @Data
  public static final class Move implements ShardMasterCommand {
    private final int groupId;
    private final int shardNum;
  }

  @Data
  public static final class Query implements ShardMasterCommand {
    private final int configNum;

    @Override
    public boolean readOnly() {
      return true;
    }
  }

  public interface ShardMasterResult extends Result {}

  @Data
  public static final class Ok implements ShardMasterResult {}

  @Data
  public static final class Error implements ShardMasterResult {}

  @Data
  public static final class ShardConfig implements ShardMasterResult {
    private final int configNum;

    // groupId -> <group members, shard numbers>
    private final Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo;
  }

  @Override
  public Result execute(Command command) {
    if (command instanceof Join) {
      Join join = (Join) command;

      // Your code here...
      if (groups.containsKey(join.groupId())) return new Error();
      groups.put(join.groupId(), join.servers());
      if (groups.size() == 1) for (int s = 1; s <= numShards; s++) shardToGroup.put(s, join.groupId());
      else rebalance();
      saveConfig();
      return new Ok();
    }

    if (command instanceof Leave) {
      Leave leave = (Leave) command;

      // Your code here...
      if (!groups.containsKey(leave.groupId())) return new Error();
      List<Integer> orphans = new ArrayList<>();
      for (Map.Entry<Integer, Integer> e : shardToGroup.entrySet()) if (e.getValue() == leave.groupId()) orphans.add(e.getKey());
      groups.remove(leave.groupId());
      for (int s : orphans) shardToGroup.remove(s);
      if (!groups.isEmpty()) distributeOrphans(orphans);
      saveConfig();
      return new Ok();      
    }

    if (command instanceof Move) {
      Move move = (Move) command;

      // Your code here...
      int shard = move.shardNum();
      int gid = move.groupId();
      if (shard < 1 || shard > numShards) return new Error();
      if (!groups.containsKey(gid)) return new Error();
      if (configs.isEmpty()) return new Error();
      Integer current = shardToGroup.get(shard);
      if (current != null && current == gid) return new Error();
      shardToGroup.put(shard, gid);
      saveConfig();
      return new Ok();
    }

    if (command instanceof Query) {
      Query query = (Query) command;

      // Your code here...
      if (configs.isEmpty()) return new Error();
      int num = query.configNum();
      if (num < 0 || num >= INITIAL_CONFIG_NUM + configs.size()) return configs.get(configs.size() - 1);
      int idx = num - INITIAL_CONFIG_NUM;
      if (idx < 0) return configs.get(configs.size() - 1);
      return configs.get(idx);
    }

    throw new IllegalArgumentException();
  }

  private void rebalance() {
    int n = groups.size();
    int base = numShards / n;
    int extra = numShards % n;
    List<Integer> groupIds = new ArrayList<>(groups.keySet());
    Collections.sort(groupIds);

    Map<Integer, Integer> targetSize = new HashMap<>();
    for (int i = 0; i < groupIds.size(); i++) targetSize.put(groupIds.get(i), base + (i < extra ? 1 : 0));

    Map<Integer, List<Integer>> groupShards = new HashMap<>();
    for (int gid : groupIds) groupShards.put(gid, new ArrayList<>());
    for (Map.Entry<Integer, Integer> e : shardToGroup.entrySet()) {
      List<Integer> list = groupShards.get(e.getValue());
      if (list != null) list.add(e.getKey());
    }
    for (List<Integer> list : groupShards.values()) Collections.sort(list);

    List<Integer> pool = new ArrayList<>();
    for (int gid : groupIds) {
      List<Integer> shards = groupShards.get(gid);
      int target = targetSize.get(gid);
      while (shards.size() > target) pool.add(shards.remove(shards.size() - 1));
    }
    for (int s = 1; s <= numShards; s++) if (!shardToGroup.containsKey(s)) pool.add(s);
    Collections.sort(pool);

    int poolIdx = 0;
    for (int gid : groupIds) {
      List<Integer> shards = groupShards.get(gid);
      int target = targetSize.get(gid);
      while (shards.size() < target && poolIdx < pool.size()) shards.add(pool.get(poolIdx++));
    }

    shardToGroup.clear();
    for (int gid : groupIds)
      for (int s : groupShards.get(gid)) shardToGroup.put(s, gid);
  }

  private void distributeOrphans(List<Integer> orphans) {
    Collections.sort(orphans);
    int n = groups.size();
    int base = numShards / n;
    int extra = numShards % n;
    List<Integer> groupIds = new ArrayList<>(groups.keySet());
    Collections.sort(groupIds);

    Map<Integer, Integer> targetSize = new HashMap<>();
    for (int i = 0; i < groupIds.size(); i++) targetSize.put(groupIds.get(i), base + (i < extra ? 1 : 0));

    Map<Integer, Integer> currentCount = new HashMap<>();
    for (int gid : groupIds) currentCount.put(gid, 0);
    for (int gid : shardToGroup.values()) currentCount.merge(gid, 1, Integer::sum);

    for (int s : orphans) {
      int bestGid = groupIds.get(0);
      int bestDeficit = targetSize.get(bestGid) - currentCount.get(bestGid);
      for (int gid : groupIds) {
        int deficit = targetSize.get(gid) - currentCount.get(gid);
        if (deficit > bestDeficit) { bestDeficit = deficit; bestGid = gid; }
      }
      shardToGroup.put(s, bestGid);
      currentCount.merge(bestGid, 1, Integer::sum);
    }
  }

  private void saveConfig() {
    int configNum = INITIAL_CONFIG_NUM + configs.size();
    Map<Integer, Pair<Set<Address>, Set<Integer>>> groupInfo = new HashMap<>();
    for (int gid : groups.keySet()) {
      Set<Integer> shards = new HashSet<>();
      for (Map.Entry<Integer, Integer> e : shardToGroup.entrySet()) if (e.getValue() == gid) shards.add(e.getKey());
      groupInfo.put(gid, new ImmutablePair<>(new HashSet<>(groups.get(gid)), shards));
    }
    configs.add(new ShardConfig(configNum, groupInfo));
  }
}
