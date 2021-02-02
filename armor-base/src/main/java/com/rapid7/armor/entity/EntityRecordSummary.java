package com.rapid7.armor.entity;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRecordSummary {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityRecordSummary.class);
  private final Object id;
  private final int numRows;
  private final int offset;
  private final long version;
  private String instanceId;

  public EntityRecordSummary(Object id, int numRows, int offset, long version, byte[] instanceId) {
    this.id = id;
    this.numRows = numRows;
    this.offset = offset;
    this.version = version;
    if (instanceId != null) {
      this.instanceId = new String(instanceId, StandardCharsets.UTF_8);
    }
    if (numRows == 0) {
      LOGGER.warn("The entity with id {} has zero rows, which shouldn't happen will be filtered out of sync", id);
    }
  }

  public String getInstanceId() {
    return instanceId;
  }

  public long getVersion() {
    return version;
  }

  public Object getId() {
    return this.id;
  }

  public int getOffset() {
    return offset;
  }

  public int getNumRows() {
    return numRows;
  }

  @Override
  public String toString() {
    return "id=" + id + ",numRows=" + numRows + ",offset=" + offset + ",version=" + version + ",instanceId=" + instanceId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, numRows, version);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other instanceof EntityRecordSummary) {
      EntityRecordSummary otherRecord = (EntityRecordSummary) other;
      return Objects.equals(id, otherRecord.id) && Objects.equals(numRows, otherRecord.numRows) && Objects.equals(version, otherRecord.version);
    }
    return false;
  }
}
