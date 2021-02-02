package com.rapid7.armor.read;

import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.columns.Column;

public class SlowArmorReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SlowArmorReader.class);
  private final ReadStore store;

  public SlowArmorReader(ReadStore store) {
    this.store = store;
  }

  public Column<?> getColumn(String org, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(org, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShard armorShard = store.getArmorShard(shardId, columnName);
    return armorShard.getColumn();
  }

  public Column<?> getColumn(String org, String table, String columnName, int limit, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(org, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShard armorShard = store.getArmorShard(shardId, columnName);
    return armorShard.getColumn().first(limit);
  }

  public Column<?> getColumn(String org, String table, String columnName) throws IOException {
    List<ShardId> shardIds = store.findShardIds(org, table, columnName).stream()
        .sorted(Comparator.comparingInt(ShardId::getShardNum))
        .collect(Collectors.toList());
    Column<?> column = null;
    for (ShardId shardId : shardIds) {
      SlowArmorShard armorShard = store.getArmorShard(shardId, columnName);
      if (column == null)
        column = armorShard.getColumn();
      else
        column.append((Column) armorShard.getColumn());
    }
    return column;
  }

  public ColumnMetadata getColumnMetadata(String org, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(org, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShard armorShard = store.getArmorShard(shardId, columnName);
    return armorShard.getMetadata();
  }
}
