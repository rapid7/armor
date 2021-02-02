package com.rapid7.armor.shard;

/**
 * Not for production. Verify hashCode is consistent across jvms, otherwise this is a false
 * premise, we should use some digest to mod over it.
 */
public class ModShardStrategy implements ShardStrategy {
  private final int numShards;

  public ModShardStrategy(int numShards) {
    this.numShards = numShards;
  }

  @Override
  public int shardNum(Object entity) {
    // Use string hashcode for consistent hashcode
    return Math.abs(entity.toString().hashCode() % numShards);
  }
}
