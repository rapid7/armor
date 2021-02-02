package com.rapid7.armor.write;

import java.util.Objects;

public class TableWriteId {
  private final String tableName;
  private final String org;

  public TableWriteId(String org, String tableName) {
    this.tableName = tableName;
    this.org = org;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, org);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other instanceof TableWriteId) {
      TableWriteId otherTableId = (TableWriteId) other;
      return Objects.equals(tableName, otherTableId.tableName) && Objects.equals(org, otherTableId.org);
    }
    return false;
  }

  @Override
  public String toString() {
    return org + ":" + tableName;
  }

}
