package com.rapid7.armor.write.writers;

import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.WriteStore;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableWriter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(TableWriter.class);

  private final String tableName;
  private final String tenant;
  private final String entityColumnId;
  private final DataType entityColumnType;
  private final WriteStore store;
  // Must be have some synchronization to prevent lost shards.
  private final Map<ShardId, ShardWriter> shards = new ConcurrentHashMap<>();

  public TableWriter(String tenant, String table, String entityColumnId, DataType dataType, WriteStore store) {
    this.store = store;
    this.tenant = tenant;
    this.tableName = table;
    this.entityColumnId = entityColumnId;
    this.entityColumnType = dataType;
  }

  public TableMetadata toTableMetadata() {
    return new TableMetadata(entityColumnId, entityColumnType.getCode());
  }

  public Collection<ShardWriter> getShardWriters() {
    return shards.values();
  }

  public String getTableName() {
    return this.tableName;
  }

  public String getTenant() {
    return this.tenant;
  }

  public String getEntityColumnId() {
    return entityColumnId;
  }

  public DataType getEntityColumnDataType() {
    return entityColumnType;
  }

  @Override
  public void close() throws IOException {
    for (ShardWriter sw : shards.values()) {
      try {
        sw.close();
      } catch (Exception e) {
        LOGGER.warn("Unable to close shard {}", sw.getShardId(), e);
      }
    }
  }

  public void close(int shard) {
    ShardId shardId = store.buildShardId(tenant, tableName, shard);
    ShardWriter sw = shards.get(shardId);
    if (sw != null)
      sw.close();
  }

  public ShardWriter getShard(int shard) {
    ShardId shardId = store.buildShardId(tenant, tableName, shard);
    return shards.get(shardId);
  }

  public void addShard(ShardWriter writer) {
    shards.put(writer.getShardId(), writer);
  }
}
