package com.rapid7.armor.shard;

import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import java.time.Instant;
import java.util.Objects;

public class ColumnShardId {
  private final ShardId shardId;
  private ColumnId columnId;

  public ColumnShardId(ShardId shardId, String columnId, DataType dataType) {
    this.shardId = shardId;
    this.columnId = new ColumnId(columnId, dataType.getCode());
  }

  public ColumnShardId(ShardId shardId, ColumnId columnId) {
    this.shardId = shardId;
    this.columnId = columnId;
  }

  public String getTenant() {
    return shardId.getTenant();
  }

  public String getTable() {
    return shardId.getTable();
  }

  public long getInterval() {
    return shardId.getInterval();
  }

  public Instant getIntervalStart() {
    return shardId.getIntervalStart();
  }

  public ColumnId getColumnId() {
    return columnId;
  }

  public void setColumnId(ColumnId columnId) {
    this.columnId = columnId;
  }

  public ShardId getShardId() {
    return shardId;
  }

  public int getShardNum() {
    return shardId.getShardNum();
  }

  public String alternateString() {
    return shardId.simpleString() + "_" + columnId.getName() + "_" + columnId.dataType();
  }

  public String toSimpleString() {
    return "shard=" + shardId + "column=" + columnId.getName() + "_" + columnId.dataType();
  }

  @Override
  public int hashCode() {
    return Objects.hash(shardId, columnId);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;

    if (other instanceof ColumnShardId) {
      ColumnShardId otherColumnShardId = (ColumnShardId) other;
      return Objects.equals(shardId, otherColumnShardId.shardId) &&
          Objects.equals(columnId, otherColumnShardId.columnId);
    }
    return false;
  }

  @Override
  public String toString() {
    return "ColumnShardId{" +
        "shardId=" + shardId +
        ", columnId=" + columnId +
        '}';
  }
}
