package com.rapid7.armor.store;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;

public interface WriteStore {
  /**
   * Returns the root directory for the write store.
   *
   * @return The rooot directory.
   */
  String rootDirectory();
  
  /**
   * Returns a list of all tenants stored.
   *
   * @return A list of all tenant store.
   */
  List<String> getTenants();
  void deleteTenant(String tenant);
  void deleteTable(String tenant, String table);
  
  ShardMetadata getShardMetadata(String tenant, String table, Interval interval, Instant timestamp, int shardNum);
  TableMetadata getTableMetadata(String tenant, String table);
  ColumnMetadata getColumnMetadata(String tenant, String table, ColumnShardId columnShard);
  
  void saveShardMetadata(String transaction, ShardMetadata shardMetadata);
  void saveTableMetadata(String transaction, TableMetadata tableMetadata);

  // Loading from store
  List<ColumnFileWriter> loadColumnWriters(String tenant, String table, Interval interval, Instant timestamp, int shardNum);
  ColumnFileWriter loadColumnWriter(ColumnShardId columnShard);



  // Get style methods
  int findShardNum(Object entityId);

  ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, Object entityId);

  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp);

  List<ColumnId> getColumnIds(ShardId shardId);

  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId);


  void copyShard(ShardId shardIdDst, ShardId shardIdSrc);

  void saveColumn(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream);

  void commit(String transaction, String tenant, String table, Interval interval, Instant timestamp, int shardNum);

  void rollback(String transaction, String tenant, String table, Interval interval, Instant timestamp, int shardNum);

  /**
   * Captures write activity either at the entity or write request level. This is useful for debugging issues and replaying
   * the writes activity afterwards for debugging or analysis.
   * 
   * @param transaction The transaction id.
   * @param shardId The shard id.
   * @param entities A list of entities to capture the write against.
   * @param writeRequests The list of write requests.
   * @param deleteRequest The delete request if used can be {@code null}.
   */
  void captureWrites(String transaction, ShardId shardId, List<Entity> entities, List<WriteRequest> writeRequests, Object deleteRequest);

  void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error);
}
