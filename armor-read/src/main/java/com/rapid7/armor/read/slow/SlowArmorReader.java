package com.rapid7.armor.read.slow;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.read.BaseArmorReader;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;

import tech.tablesaw.api.Table;
import tech.tablesaw.columns.Column;

/**
 * Slow armor reader is a slower version of the reader. Use this explore the data if running locally and you need
 * to look at data via an easy to use API.
 */
public class SlowArmorReader extends BaseArmorReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SlowArmorReader.class);

  public SlowArmorReader(ReadStore store) {
    super(store);
  }

  public Column<?> getColumn(String tenant, String table, String columnName, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnName);
    return armorShard.getColumn();
  }

  public Column<?> getColumn(String tenant, String table, String columnName, int limit, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnName);
    return armorShard.getColumn().first(limit);
  }

  public Column<?> getColumn(String tenant, String table, String columnName) throws IOException {
    List<ShardId> shardIds = store.findShardIds(tenant, table, columnName).stream()
        .sorted(Comparator.comparingInt(ShardId::getShardNum))
        .collect(Collectors.toList());
    Column<?> column = null;
    for (ShardId shardId : shardIds) {
      SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnName);
      if (column == null)
        column = armorShard.getColumn();
      else
        column.append((Column) armorShard.getColumn());
    }
    return column;
  }
  
  public Table getEntity(String tenant, String table, Object entity) {
    List<ColumnName> columnNames = store.getColumNames(tenant, table);
    Map<String, Column<?>> columns = new HashMap<>();
    for (ShardId shardId : store.findShardIds(tenant, table)) {
      for (ColumnName columnName : columnNames) {
        SlowArmorShardColumn sas = store.getSlowArmorShard(shardId, columnName.getName());
        Column<?> column = sas.getColumnByEntityId(entity);
        if (columns.containsKey(column.name())) {
          columns.get(column.name()).append((Column) column);
        } else {
          columns.put(column.name(), column);
        } 
      }
    }
    return Table.create("", columns.values());
  }
}
