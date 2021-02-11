package com.rapid7.armor.read.slow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rapid7.armor.read.BaseArmorReader;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.ReadStore;

import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.StringColumn;
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

  public Column<?> getColumn(String tenant, String table, String columnId, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnId);
    return armorShard.getColumn();
  }

  public Column<?> getColumn(String tenant, String table, String columnId, int limit, int shardNum) throws IOException {
    ShardId shardId = store.findShardId(tenant, table, shardNum);
    if (shardId == null)
      return null;
    SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnId);
    return armorShard.getColumn().first(limit);
  }

  public Column<?> getColumn(String tenant, String table, String columnId) throws IOException {
    List<ShardId> shardIds = store.findShardIds(tenant, table, columnId).stream()
        .sorted(Comparator.comparingInt(ShardId::getShardNum))
        .collect(Collectors.toList());
    Column<?> column = null;
    for (ShardId shardId : shardIds) {
      SlowArmorShardColumn armorShard = store.getSlowArmorShard(shardId, columnId);
      if (column == null)
        column = armorShard.getColumn();
      else
        column.append((Column) armorShard.getColumn());
    }
    return column;
  }
  
  public Table getTable(String tenant, String table) {
    List<ColumnId> columnIds = store.getColumnIds(tenant, table);
    Map<String, Column<?>> columns = new HashMap<>();
    for (ShardId shardId : store.findShardIds(tenant, table)) {
      Set<ColumnId> nullColumns = new HashSet<>();
      int shardRows = -1;
      for (ColumnId columnId : columnIds) {
        SlowArmorShardColumn sas = store.getSlowArmorShard(shardId, columnId.getName());
        if (sas == null) {
          // If it is null, then this is most likely due to a new column introduced in another shard.
          // Record this then create an empty column equivalen to this shard's row count and set as all null.
          nullColumns.add(columnId);
          continue;
        }
        Column<?> column = sas.getColumn();
        if (shardRows < 0)
          shardRows = column.size();
        if (columns.containsKey(column.name())) {
          columns.get(column.name()).append((Column) column);
        } else {
          columns.put(column.name(), column);
        }
      }
      for (ColumnId nullColumnId : nullColumns) {
        Column<?> column = null;
        if (nullColumnId.dataType() == DataType.STRING) {
          StringColumn strColumn = StringColumn.create(nullColumnId.getName());
          ArrayList<String> values = new ArrayList<>();
          for (int i = 0; i < shardRows; i++)
            values.add(null);
          strColumn.addAll(values);
          column = strColumn;
        } else if (nullColumnId.dataType() == DataType.INTEGER) {
          IntColumn intColumn = IntColumn.create(nullColumnId.getName());
          for (int i = 0; i < shardRows; i++)
            intColumn.add(Integer.MIN_VALUE);
          column = intColumn;
        } else if (nullColumnId.dataType() == DataType.LONG || nullColumnId.dataType() == DataType.DATETIME) {
          LongColumn longColumn = LongColumn.create(nullColumnId.getName());
          for (int i = 0; i < shardRows; i++)
            longColumn.add(Long.MIN_VALUE);
          column = longColumn;
        } else if (nullColumnId.dataType() == DataType.FLOAT) {
          FloatColumn floatColumn = FloatColumn.create(nullColumnId.getName());
          for (int i = 0; i < shardRows; i++)
            floatColumn.add(Float.NaN);
          column = floatColumn;
        } else {
          throw new RuntimeException("The datatype " + nullColumnId.dataType() + " isn't supported yet");
        }
        if (columns.containsKey(column.name())) {
          columns.get(column.name()).append((Column) column);
        } else {
          columns.put(column.name(), column);
        }
      }
    }
    return Table.create(table, columns.values());
  }
  
  public Table getEntity(String tenant, String table, Object entity) {
    List<ColumnId> columnIds = store.getColumnIds(tenant, table);
    Map<String, Column<?>> columns = new HashMap<>();
    for (ShardId shardId : store.findShardIds(tenant, table)) {
      for (ColumnId columnId : columnIds) {
        SlowArmorShardColumn sas = store.getSlowArmorShard(shardId, columnId.getName());
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
