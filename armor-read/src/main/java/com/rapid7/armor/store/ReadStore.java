package com.rapid7.armor.store;

import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ShardId;
import java.time.Instant;
import java.util.List;

public interface ReadStore {
  // Schema related apis
  /**
   * Returns all the tenants visible to this store.
   * 
   * @return A list of tenants.
   */
  List<String> getTenants();

  /**
   * Returns a list of intervals that the table has.
   * 
   * @param tenant The tenant to find.
   * @param table The table to find.
   *
   * @return A list of {@link Interval}s.
   */
  List<Interval> getIntervals(String tenant, String table);
  
  /**
   * Returns a listing of all the tables for a tenant.
   *
   * @param tenant The tenant to find.
   *
   * @return A list of table names.
   */
  List<String> getTables(String tenant);

  // Interval starts
  /**
   * Returns a list of interval start times for a given interval.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param predicate A search predicate to filter against, can be {@code null}.
   *
   * @return A list of start intervals in ISO-8601 format.
   */
  List<String> getIntervalStarts(String tenant, String table, Interval interval, InstantPredicate predicate);
  
  /**
   * Returns a list of start intervals for a given interval.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   *
   * @return A list of start intervals in ISO-8601 format.
   */
  List<String> getIntervalStarts(String tenant, String table, Interval interval);

  // Metadata apis
  /**
   * Gets the table metadata.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   *
   * @return The {@link TableMetadata} or {@code null}.
   */
  TableMetadata getTableMetadata(String tenant, String table);

  /**
   * Gets the shard metadata.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param timestamp The timestamp with the interval to check against.
   * @param shardNum The shard number.
   *
   * @return The {@link ShardMetadata} or {@code null}.
   */
  ShardMetadata getShardMetadata(String tenant, String table, Interval interval, Instant timestamp, int shardNum);

  // Reader apis
  SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnName);
  FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnName);

  // Column id apis
  ColumnId getColumnId(String tenant, String table, Interval interval, Instant timestamp, String columnName);
  List<ColumnId> getColumnIds(String tenant, String table);
  List<ColumnId> getColumnIds(String tenant, String table, Interval interval, Instant timestamp);
  List<ColumnId> getColumnIds(ShardId shardId);
  

  // Shard apis
  /**
   * Finds the {@link ShardId} given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param timestamp The timestamp witihn the interval.
   * @param shardNum The shard number.
   *
   * @return The {@link ShardId} or {@code null}.
   */
  ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, int shardNum);

  /**
   * Finds the {@link ShardId}s given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param timestamp The timestamp witihn the interval.
   *
   * @return A list of {@link ShardId}s.
   */
  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp);
  
  /**
   * Finds the {@link ShardId}s given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param timestamp The timestamp within the interval.
   * @param columnName The name of the column.
   *
   * @return A list of {@link ShardId}s.
   */
  List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnName);
  
  /**
   * Finds the {@link ShardId}s given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   * @param intervalStartPredicate The intervaL start predicate to filter on, can be {@code null}.
   *
   * @return A list of {@link ShardId}s.
   */
  List<ShardId> findShardIds(String tenant, String tableName, Interval interval, InstantPredicate intervalStartPredicate);
  
  /**
   * Finds the {@link ShardId}s given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param intervalPredicate The interval predicate to check against, can be {@code null}.
   * @param intervalStartPredicate The intervaL start predicate to filter on, can be {@code null}.
   *
   * @return A list of {@link ShardId}s.
   */
  List<ShardId> findShardIds(String tenant, String tableName, StringPredicate intervalPredicate, InstantPredicate intervalStartPredicate);
  
  /**
   * Finds the {@link ShardId}s given a set of parameters.
   *
   * @param tenant The tenant to find.
   * @param table The table to find.
   * @param interval The interval to check against.
   *
   * @return A list of {@link ShardId}s.
   */
  List<ShardId> findShardIds(String tenant, String tableName, Interval interval);
}
