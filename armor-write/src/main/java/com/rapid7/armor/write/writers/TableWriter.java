package com.rapid7.armor.write.writers;

import com.rapid7.armor.shard.ShardId;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableWriter.class);

  private final String tableName;
  private final String tenant;
  // Must be have some synchronization to prevent lost shards.
  private final Map<ShardId, IShardWriter> shards = new HashMap<>();

  public TableWriter(String tenant, String table) {
    this.tenant = tenant;
    this.tableName = table;
  }

  public Collection<IShardWriter> getShardWriters() {
    return shards.values();
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getTenant() {
    return this.tenant;
  }

  @Override
  public synchronized void close() {
    for (IShardWriter sw : shards.values()) {
      try {
        sw.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close shard {}", sw.getShardId(), e);
      }
    }
  }

  public synchronized void close(ShardId shardId) {
    IShardWriter sw = shards.get(shardId);
    if (sw != null) {
      try {
        sw.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public IShardWriter getShard(ShardId shardId) {
    return shards.get(shardId);
  }

  public synchronized IShardWriter addShard(IShardWriter shardWriter) {
    IShardWriter sw = shards.get(shardWriter.getShardId());
    if (sw != null) {
      // A shard already exists, so close it and move on.
      try {
        shardWriter.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
      return sw;
    }
    shards.put(shardWriter.getShardId(), shardWriter);
    return shardWriter;
  }
}
