package com.rapid7.armor.write;

import java.util.Objects;

public class TableId {
  private final String tableName;
  private final String tenant;

  public TableId(String tenant, String tableName) {
    this.tableName = tableName;
    this.tenant = tenant;
  }

  public String getTableName() {
    return tableName;
  }
  
  public String getTenant() {
    return tenant;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, tenant);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other instanceof TableId) {
      TableId otherTableId = (TableId) other;
      return Objects.equals(tableName, otherTableId.tableName) && Objects.equals(tenant, otherTableId.tenant);
    }
    return false;
  }

  @Override
  public String toString() {
    return tenant + ":" + tableName;
  }

}
