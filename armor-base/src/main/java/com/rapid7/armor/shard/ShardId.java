package com.rapid7.armor.shard;

import java.util.Objects;

public class ShardId {

  private final String table;
  private final String org;
  private final int shardNum;

  public ShardId(int shardNum, String org, String table) {
    this.shardNum = shardNum;
    this.org = org;
    this.table = table;
  }

  public String getTable() {
    return table;
  }

  public String getOrg() {
    return org;
  }

  public int getShardNum() {
    return shardNum;
  }

  @Override
  public String toString() {
    return "ShardId{" +
        "table='" + table + '\'' +
        ", org='" + org + '\'' +
        ", shardNum=" + shardNum +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ShardId shardId = (ShardId) o;
    return getShardNum() == shardId.getShardNum() && Objects.equals(getTable(), shardId.getTable()) && Objects.equals(getOrg(), shardId.getOrg());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTable(), getOrg(), getShardNum());
  }

  public String simpleString() {
    return org + "_" + table + "_" + shardNum;
  }

  public String getShardId() {
    return org + "/" + table + "/" + shardNum;
  }

}
