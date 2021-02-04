package com.rapid7.armor.read;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;

/**
 */
public class FastArmorReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastArmorReader.class);
  private final ReadStore store;

  public FastArmorReader(ReadStore store) {
    this.store = store;
  }

  public FastArmorColumnReader getColumn(String org, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(org, table, shardNum);
    if (shardId == null)
      return null;
    FastArmorShard armorShard = store.getFastArmorShard(shardId, columnName);
    return armorShard.getFastArmorColumnReader();
  }

  public ColumnMetadata getColumnMetadata(String org, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(org, table, shardNum);
    if (shardId == null)
      return null;
    FastArmorShard armorShard = store.getFastArmorShard(shardId, columnName);
    return armorShard.getMetadata();
  }
}
