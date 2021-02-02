package com.rapid7.armor.shard;

import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import java.util.Objects;

public class ColumnShardId {
  private final ShardId shardId;
  private ColumnName columnName;

  public ColumnShardId(ShardId shardId, String columnName, DataType dataType) {
    this.shardId = shardId;
    this.columnName = new ColumnName(columnName, dataType.getCode());
  }

  public ColumnShardId(ShardId shardId, ColumnName columnName) {
    this.shardId = shardId;
    this.columnName = columnName;
  }

  public ShardId shardId() {
    return shardId;
  }

  public String getOrg() {
    return shardId.getOrg();
  }

  public String getTable() {
    return shardId.getTable();
  }

  public ColumnName getColumnName() {
    return columnName;
  }

  public void setColumnName(ColumnName columnName) {
    this.columnName = columnName;
  }

  public ShardId getShardId() {
    return shardId;
  }

  public int getShardNum() {
    return shardId.getShardNum();
  }

  public String alternateString() {
    return shardId.simpleString() + "_" + columnName.getName() + "_" + columnName.dataType();
  }

  public String toSimpleString() {
    return "shard=" + shardId + "column=" + columnName.getName() + "_" + columnName.dataType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, columnName);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;

    if (other instanceof ColumnShardId) {
      ColumnShardId otherColumnShardId = (ColumnShardId) other;
      return Objects.equals(shardId, otherColumnShardId.shardId) &&
          Objects.equals(columnName, otherColumnShardId.columnName);
    }
    return false;
  }

  @Override
  public String toString() {
    return "ColumnShardId{" +
        "shardId=" + shardId +
        ", columnName=" + columnName +
        '}';
  }
}
