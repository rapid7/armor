package com.rapid7.armor.store;

import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;
import java.util.List;
import java.util.Map;

public interface ReadStore {
  List<String> getTenants();

  ColumnId findColumnId(String tenant, String table, String columnName);
  
  ShardMetadata getShardMetadata(String tenant, String table, int shardNum);
  
  ShardId findShardId(String tenant, String table, int shardNum);

  SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnName);

  FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnName);

  List<ColumnId> getColumnIds(String tenant, String table);

  List<ColumnId> getColumnIds(ShardId shardId);

  List<ShardId> findShardIds(String tenant, String table);

  List<ShardId> findShardIds(String tenant, String table, String columnId);

  List<String> getTables(String tenant);

  String resolveCurrentPath(String tenant, String table, int shardNum);

  Map<String, String> getCurrentValues(String tenant, String table, int shardNum);
}
