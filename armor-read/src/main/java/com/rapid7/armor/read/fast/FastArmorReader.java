package com.rapid7.armor.read.fast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.BaseArmorReader;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;

/**
 * The fast armor reader is primarily focused on highly optimized reads. This is use for production systems such as
 * presto to use.
 */
public class FastArmorReader extends BaseArmorReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastArmorReader.class);

  public FastArmorReader(ReadStore store) {
    super(store);
  }

  public FastArmorBlockReader getColumn(ShardId shardId, String columnName) throws IOException {
    if (!store.shardIdExists(shardId))
      return new NullArmorBlockReader(0);
    FastArmorShardColumn armorShard = store.getFastArmorShard(shardId, columnName);
    if (armorShard == null) {
      ShardMetadata metadata = store.getShardMetadata(shardId);
      if (metadata == null)
        return new NullArmorBlockReader(0);
      int numRows = metadata.getColumnMetadata().get(0).getNumRows();
      // If its null, then we assume the column does exist but perhaps another shard has that column, in that case we should return
      // a block of null values.
      return new NullArmorBlockReader(numRows);
    }
    return armorShard.getFastArmorColumnReader();
  }

  public FastArmorBlockReader getFixedValueColumn(ShardId shardId, Object fixedValue) throws IOException {
    if (!store.shardIdExists(shardId))
      return null;
    ShardMetadata metadata = store.getShardMetadata(shardId);
    if (metadata == null) {
        LOGGER.error("The metadata for {} is empty", shardId);
        throw new RuntimeException("The metadata for shard " + shardId + " is empty");
    }
    
    if (metadata.getColumnMetadata() == null || metadata.getColumnMetadata().isEmpty()) {
        LOGGER.error("The metadata for {} is has no column metadata", shardId);
        throw new RuntimeException("The metadata for shard " + shardId + " has no column metadata");
    }
    int numRows = metadata.getColumnMetadata().get(0).getNumRows();

    return new FixedValueArmorBlockReader(fixedValue, numRows);
  }
}
