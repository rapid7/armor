package com.rapid7.armor.shard;

import java.nio.file.Path;
import java.util.Objects;

public class ShardId {

  private final String table;
  private final String tenant;
  private final String interval;
  private final String intervalStart;
  private final int shardNum;

  public ShardId(String tenant, String table, String interval, String intervalStart, int shardNum) {
    this.tenant = tenant;
    this.table = table;
    this.interval = interval;
    this.intervalStart = intervalStart;
    this.shardNum = shardNum;
  }

  public ShardId(Path columnFile) {
    this.tenant = columnFile.getParent().getParent().getParent().getParent().getParent().getFileName().toString();
    this.table = columnFile.getParent().getParent().getParent().getParent().getFileName().toString();
    this.interval = columnFile.getParent().getParent().getParent().getFileName().toString();
    this.intervalStart = columnFile.getParent().getParent().getFileName().toString();
    this.shardNum = Integer.parseInt(columnFile.getParent().getFileName().toString());
  }

  public String getTable() {
    return table;
  }

  public String getTenant() {
    return tenant;
  }

  public String getInterval() {
    return interval;
  }

  public String getIntervalStart() {
    return intervalStart;
  }

  public int getShardNum() {
    return shardNum;
  }

  @Override
  public String toString() {
    return "ShardId{" +
        "tenant='" + tenant + '\'' +
        ", table='" + table + '\'' +
        ", interval=" + interval + '\'' +
        ", intervalStart=" + intervalStart + '\'' +
        ", shardNum=" + shardNum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShardId shardId = (ShardId) o;
    return getShardNum() == shardId.getShardNum() && Objects.equals(getTable(), shardId.getTable()) && Objects.equals(getTenant(), shardId.getTenant()) && Objects.equals(getInterval(), shardId.getInterval()) && Objects.equals(getIntervalStart(), shardId.getIntervalStart());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTable(), getTenant(), getInterval(), getIntervalStart(), getShardNum());
  }

  public String simpleString() {
    return tenant + "_" + table  + "_" + interval + "_" + intervalStart + "_" + shardNum;
  }

  public String getShardId() {
    return tenant + "/" + table  + "/" + interval + "/" + intervalStart + "/" + shardNum;
  }

}
