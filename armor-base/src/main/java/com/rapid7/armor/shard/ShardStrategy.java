package com.rapid7.armor.shard;

public interface ShardStrategy {
  int shardNum(Object entity);
}
