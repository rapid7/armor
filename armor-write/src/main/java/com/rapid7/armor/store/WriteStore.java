package com.rapid7.armor.store;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.rapid7.armor.xact.ArmorXact;

import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Set;

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
   * @param useCache attempt to use a cached list of tenants.
   *
   * @return A list of all tenant store.
   */
  List<String> getTenants(boolean useCache);
  /**
   * Delete a tenant.
   *
   * @param tenant The tenant to delete.
   */
  void deleteTenant(String tenant);
  /**
   * Deletes a table.
   *
   * @param tenant The tenant to find.
   * @param table The table to delete.
   */
  void deleteTable(String tenant, String table);
  
  /**
   * Deletes a table's interval.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to delete.
   */
  void deleteInterval(String tenant, String table, Interval interval);
  /**
   * Returns whether the table at interval exists.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to find.
   * 
   * @return {@code true} if it exists.
   */
  boolean intervalExists(String tenant, String table, Interval interval);
  /**
   * Returns whether the table at interval exists.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to find.
   * @param intervalStart The start interval in ISO8601 format.
   *
   * @return {@code true} if it exists.
   */
  boolean intervalStartExists(String tenant, String table, Interval interval, String intervalStart);
  /**
   * Returns whether the table at interval exists.
   *
   * @param columnShardId The column shard exists.
   *
   * @return {@code true} if it exists.
   */
  boolean columnShardIdExists(ColumnShardId columnShardId);
  
  /**
   * Returns whether the table exists.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   *
   * @return {@code true} if it exists.
   */
  boolean tableExists(String tenant, String table);

  /**
   * Deletes a table's interval based off matching interval start.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to match.
   * @param intervalStart The start interval in ISO8601 format.
   */
  void deleteIntervalStart(String tenant, String table, Interval interval, String intervalStart);
  
  ShardMetadata getShardMetadata(ShardId shardId);
  ColumnMetadata getColumnMetadata(String tenant, String table, ColumnShardId columnShard);
  
  void saveShardMetadata(ArmorXact transaction, ShardMetadata shardMetadata);
  ColumnId getEntityIdColumn(String tenant, String table);
  void saveTableMetadata(String tenant, String table, Set<ColumnId> columnIds, ColumnId entityColumnId);
  void saveColumnMetadata(String tenant, String table, ColumnId column, boolean isEntityColumn);

  // Loading from store
  List<ColumnFileWriter> loadColumnWriters(ShardId shardId);
  ColumnFileWriter loadColumnWriter(ColumnShardId columnShard);

  // Get style methods
  int findShardNum(Object entityId);

  ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, Object entityId);

  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp);

  List<ColumnId> getColumnIds(ShardId shardId);

  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId);


  void copyShard(ShardId shardIdDst, ShardId shardIdSrc);

  void saveColumn(ArmorXact armorTranscation, ColumnShardId columnShardId, int size, InputStream inputStream);

  // Transaction semantics
  ArmorXact begin(String transaction, ShardId shard);
  void commit(ArmorXact armorTransaction, ShardId shardId);
  void rollback(ArmorXact armorTranscation, ShardId shardId);

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
  void captureWrites(ArmorXact transaction, ShardId shardId, List<Entity> entities, List<WriteRequest> writeRequests, Object deleteRequest);

  void saveError(ArmorXact transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error);
}
