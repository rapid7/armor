package com.rapid7.armor.shard;

import java.util.Objects;

public class ShardId {

  private final String table;
  private final String tenant;
  private final int shardNum;

  public ShardId(int shardNum, String tenant, String table) {
    this.shardNum = shardNum;
    this.tenant = tenant;
    this.table = table;
  }

  public String getTable() {
    return table;
  }

  public String getTenant() {
    return tenant;
  }

  public int getShardNum() {
    return shardNum;
  }

  @Override
  public String toString() {
    return "ShardId{" +
        "table='" + table + '\'' +
        ", tenant='" + tenant + '\'' +
        ", shardNum=" + shardNum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShardId shardId = (ShardId) o;
    return getShardNum() == shardId.getShardNum() && Objects.equals(getTable(), shardId.getTable()) && Objects.equals(getTenant(), shardId.getTenant());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTable(), getTenant(), getShardNum());
  }

  public String simpleString() {
    return tenant + "_" + table + "_" + shardNum;
  }

  public String getShardId() {
    return tenant + "/" + table + "/" + shardNum;
  }

}
