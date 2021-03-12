package com.rapid7.armor.shard;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.PathBuilder;

public class ShardId {

  private String table;
  private String tenant;
  private String interval;
  private String intervalStart;
  private int shardNum;

  public static ShardId buildShardId(String tenant, String table, Interval interval, Instant timestamp, int shardNum) {
    return new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);
  }
  
  public static ShardId buildPreviousIntervalShardId(ShardId shardId) {
    Interval interval = Interval.toInterval(shardId.getInterval());
    String intervalStart = shardId.getIntervalStart();
    String previousIntervalStart = interval.getIntervalStart(Instant.parse(intervalStart), -1);
    ShardId previousShardId = new ShardId(shardId);
    previousShardId.setIntervalStart(previousIntervalStart);
    return previousShardId;
  }

  public ShardId() {}

  public ShardId(ShardId copy) {
    this.tenant = copy.tenant;
    this.table = copy.table;
    this.interval = copy.interval;
    this.intervalStart = copy.intervalStart;
    this.shardNum = copy.shardNum;
  }

  public ShardId(String tenant, String table, String interval, String intervalStart, int shardNum) {
    this.tenant = tenant;
    this.table = table;
    this.interval = interval;
    this.intervalStart = intervalStart;
    this.shardNum = shardNum;
  }

  public ShardId(Path columnFile) {
    this.tenant = columnFile.getParent().getParent().getParent().getParent().getParent().getParent().getFileName().toString();
    this.table = columnFile.getParent().getParent().getParent().getParent().getParent().getFileName().toString();
    this.interval = columnFile.getParent().getParent().getParent().getParent().getFileName().toString();
    this.intervalStart = columnFile.getParent().getParent().getParent().getFileName().toString();
    this.shardNum = Integer.parseInt(columnFile.getParent().getParent().getFileName().toString());
  }
  
  public static ShardId parse(Path columFile, Path base) {
    return new ShardId(base.relativize(columFile));
  }

  public void setTable(String table) {
    this.table = table;
  }
  
  public String getTable() {
    return table;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public String getTenant() {
    return tenant;
  }
  
  public void setInterval(String interval) {
    this.interval = interval;
  }

  public String getInterval() {
    return interval;
  }

  public void setIntervalStart(String intervalStart) {
    this.intervalStart = intervalStart;
  }

  public String getIntervalStart() {
    return intervalStart;
  }

  public void setShardNum(int shardNum) {
    this.shardNum = shardNum;
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

  public String shardIdPath() {
    return PathBuilder.buildPath(tenant, table, interval, intervalStart, Integer.toString(shardNum));
  }

}
