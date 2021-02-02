package com.rapid7.armor.write;

import com.rapid7.armor.entity.Column;
import java.util.Objects;

public class WriteRequest {
  private Object entityId;
  private long version;
  private String instanceid;
  private Column column;

  public WriteRequest() {
  }

  public WriteRequest(Object entityId, long version, String instanceid, Column column) {
    this.entityId = entityId;
    this.version = version;
    this.column = column;
    this.instanceid = instanceid;
  }

  public Column getColumn() {
    return column;
  }

  public void setColumn(Column column) {
    this.column = column;
  }

  public Object getEntityId() {
    return entityId;
  }

  public void setEntityId(Object entityId) {
    this.entityId = entityId;
  }

  public Object[] values() {
    return column.values();
  }

  public String getInstanceId() {
    return instanceid;
  }

  public void setInstanceId(String instanceId) {
    this.instanceid = instanceId;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    WriteRequest that = (WriteRequest) o;
    return getVersion() == that.getVersion() && Objects.equals(getEntityId(), that.getEntityId()) && Objects.equals(instanceid, that.instanceid) && Objects.equals(getColumn(), that.getColumn());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getEntityId(), getVersion(), instanceid, getColumn());
  }
}
