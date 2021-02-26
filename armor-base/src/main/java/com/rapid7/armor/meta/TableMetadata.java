package com.rapid7.armor.meta;

import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@JsonInclude(value = Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetadata {
  private String entityColumnId;
  private String entityColumnIdType;
  private List<ShardMetadata> shardMetadata = new ArrayList<>();

  public TableMetadata() {}

  public TableMetadata(String entityColumnId, String entityColumnIdType) {
    this.entityColumnId = entityColumnId;
    this.entityColumnIdType = entityColumnIdType;
  }

  public List<ShardMetadata> getShardMetadata() {
    return shardMetadata;
  }

  public void setShardMetadata(List<ShardMetadata> shardMetadata) {
    this.shardMetadata = shardMetadata;
  }

  public String getEntityColumnId() {
    return entityColumnId;
  }

  public void setEntityColumnId(String entityColumnId) {
    this.entityColumnId = entityColumnId;
  }

  public DataType entityIdColumnType() {
    return DataType.getDataType(entityColumnIdType);
  }

  public String getEntityColumnIdType() {
    return entityColumnIdType;
  }

  public void setEntityColumnIdType(String entityColumnIdType) {
    this.entityColumnIdType = entityColumnIdType;
  }
  
  public List<ColumnId> getColumnIds() {
    Set<ColumnId> columnIds = new HashSet<>(); 
    for (ShardMetadata shardMetadata : shardMetadata) {
      columnIds.addAll(
         shardMetadata.getColumnMetadata().stream().map(c -> new ColumnId(c.getColumnName(), c.getColumnType().getCode())).collect(Collectors.toSet()));
    }
    return new ArrayList<>(columnIds);
  }
}
