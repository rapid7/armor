package com.rapid7.armor.read.fast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.read.BaseArmorReader;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;

/**
 * The fast armor reader is primarliy focused on highly optimized reads. This is use for production systems such as
 * presto to use.
 */
public class FastArmorReader extends BaseArmorReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastArmorReader.class);

  public FastArmorReader(ReadStore store) {
    super(store);
  }

  public FastArmorBlockReader getColumn(String tenant, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, shardNum);
    if (shardId == null)
      return null;
    FastArmorShardColumn armorShard = store.getFastArmorShard(shardId, columnName);
    if (armorShard == null)
      return null;
    return armorShard.getFastArmorColumnReader();
  }
}
