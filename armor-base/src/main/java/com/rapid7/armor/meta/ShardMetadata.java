package com.rapid7.armor.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ShardId;

import java.util.List;
import java.util.stream.Collectors;

@JsonInclude(value = Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardMetadata {

  private List<ColumnMetadata> columnMetadatas;
  private ShardId shardId;

  public ShardMetadata() {} 
  
  public ShardMetadata(ShardId shardId, List<ColumnMetadata> columnMetadatas) {
    this.shardId = shardId;
    this.columnMetadatas = columnMetadatas;
  }

  public List<ColumnMetadata> getColumnMetadata() {
    return columnMetadatas;
  }

  public void setColumnMetadata(List<ColumnMetadata> columnMetadata) {
    this.columnMetadatas = columnMetadata;
  }
  
  public ShardId setShardId(ShardId shardId) {
    return shardId;
  }

  public ShardId getShardId() {
    return shardId;
  }
  
  public List<ColumnId> columnIds() {
    return columnMetadatas.stream()
        .filter(c -> c != null)
        .map(c -> new ColumnId(c.getColumnName(), c.getColumnType().getCode()))
        .collect(Collectors.toList()); 
  }
}
