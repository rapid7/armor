package com.rapid7.armor.shard;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import static com.rapid7.armor.schema.Interval.INTERVAL_UNITS;

public class ShardId {

  private final String table;
  private final String tenant;
  private final long interval;
  private final long intervalSlice;
  private final int shardNum;

  public ShardId(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    this.tenant = tenant;
    this.table = table;
    this.interval = interval;
    this.intervalSlice = timestamp.toEpochMilli() / this.interval / INTERVAL_UNITS;
    this.shardNum = shardNum;
  }

  public ShardId(Path columnFile) {
    this.tenant = columnFile.getParent().getParent().getParent().getParent().getParent().getFileName().toString();
    this.table = columnFile.getParent().getParent().getParent().getParent().getFileName().toString();
    this.interval = Long.parseLong(columnFile.getParent().getParent().getParent().getFileName().toString());
    String timestamp = columnFile.getParent().getParent().getFileName().toString();
    this.intervalSlice = Instant.parse(timestamp).toEpochMilli() / this.interval / INTERVAL_UNITS;
    this.shardNum = Integer.parseInt(columnFile.getParent().getFileName().toString());
  }

  public String getTable() {
    return table;
  }

  public String getTenant() {
    return tenant;
  }

  public long getInterval() {
    return interval;
  }

  public long getIntervalSlice() {
    return intervalSlice;
  }

  public Instant getIntervalStart() {
    return Instant.ofEpochMilli(interval * intervalSlice * INTERVAL_UNITS);
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
        ", intervalStart=" + getIntervalStart() + '\'' +
        ", shardNum=" + shardNum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShardId shardId = (ShardId) o;
    return getInterval() == shardId.getInterval() && getIntervalSlice() == shardId.getIntervalSlice() && getShardNum() == shardId.getShardNum() && Objects.equals(getTable(), shardId.getTable()) && Objects.equals(getTenant(), shardId.getTenant());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTable(), getTenant(), getInterval(), getIntervalSlice(), getShardNum());
  }

  public String simpleString() {
    return tenant + "_" + table  + "_" + interval + "_" + getIntervalStart() + "_" + shardNum;
  }

  public String getShardId() {
    return tenant + "/" + table  + "/" + interval + "/" + getIntervalStart() + "/" + shardNum;
  }

}
