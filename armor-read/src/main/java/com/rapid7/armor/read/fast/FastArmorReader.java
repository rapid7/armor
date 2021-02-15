package com.rapid7.armor.read.fast;

import java.io.IOException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ShardMetadata;
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

  public FastArmorBlockReader getColumn(String tenant, String table, Interval interval, Instant timestamp, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, interval, timestamp, shardNum);
    if (shardId == null)
      return null;
    FastArmorShardColumn armorShard = store.getFastArmorShard(shardId, columnName);
    if (armorShard == null) {
      ShardMetadata metadata = store.getShardMetadata(tenant, table, interval, timestamp, shardNum);
      if (metadata == null)
        return null;
      int numRows = metadata.getColumnMetadata().get(0).getNumRows();
      // If its null, then we assume the column does exist but perhaps another shard has that column, in that case we should return
      // a block of null values.
      NullArmorBlockReader nabr = new NullArmorBlockReader(numRows);      
      return nabr;
    }
    return armorShard.getFastArmorColumnReader();
  }
}
