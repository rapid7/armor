package com.rapid7.armor.store;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.ColumnWriter;
import com.rapid7.armor.write.WriteRequest;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

public interface WriteStore {
  // Loading from store
  List<ColumnWriter> loadColumnWriters(String org, String table, int shardNum);

  ColumnWriter loadColumnWriter(ColumnShardId columnShard);

  ShardMetadata loadShardMetadata(String org, String table, int shardNum);

  TableMetadata loadTableMetadata(String org, String table);

  // Get style methods
  int findShardNum(Object entityId);

  ShardId findShardId(String org, String table, Object entityId);

  ShardId buildShardId(String org, String table, int shardNum);

  List<ShardId> findShardIds(String org, String table);

  List<ColumnName> getColumNames(ShardId shardId);

  List<ShardId> findShardIds(String org, String table, String columName);

  String resolveCurrentPath(String org, String table, int shardNum);

  Map<String, String> getCurrentValues(String org, String table, int shardNum);

  // Write to store
  void saveCurrentValues(String org, String table, int shardNum, String currentTransaction, String previousTransaction);

  void saveShardMetadata(String transaction, String org, String table, int shardNum, ShardMetadata shardMetadata);

  void saveTableMetadata(String transaction, String org, String table, TableMetadata tableMetadata);

  void saveColumn(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream);

  void commit(String transaction, String org, String table, int shardNum);

  void rollback(String transaction, String org, String table, int shardNum);

  /**
   * Captures write activity either at the entity or write request level. This is useful for debugging issues and replaying
   * the writes activity afterwards for debugging or analysis.
   */
  void captureWrites(String transaction, ShardId shardId, List<Entity> entities, List<WriteRequest> writeRequests, Object deleteRequest);

  void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error);
}
