package com.rapid7.armor.entity;

import com.rapid7.armor.Constants;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityRecord {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityRecord.class);
  private static final Comparator<EntityRecord> OFFSET_COMPARATOR_BY_ID = Comparator.comparing(EntityRecord::getRowGroupOffset)
      .thenComparing(EntityRecord::totalLength)
      .thenComparing(EntityRecord::getEntityId);
  private static final Comparator<EntityRecord> OFFSET_COMPARATOR = Comparator.comparing(EntityRecord::getRowGroupOffset)
      .thenComparing(EntityRecord::totalLength);
  private final int entityId;
  private int rowGroupOffset;
  private int valueLength;
  private final long version;
  private byte[] instanceUuid;
  private byte deleted;
  private int nullLength;
  private int decodedLength;

  public EntityRecord(
      int entityId, int rowGroupOffset, int valueLength, long version, byte deleted, int nullLength, int decodedLength, byte[] instanceUuid) {
    this.entityId = entityId;
    this.rowGroupOffset = rowGroupOffset;
    this.valueLength = valueLength;
    this.version = version;
    this.deleted = deleted;
    this.nullLength = nullLength;
    this.decodedLength = decodedLength;
    if (instanceUuid == null)
      this.instanceUuid = new byte[Constants.INSTANCE_ID_BYTE_LENGTH];
    else {
      if (instanceUuid.length > Constants.INSTANCE_ID_BYTE_LENGTH) {
        LOGGER.warn("The given instance id is longer than 36 bytes restricting to first 36 bytes on {}", entityId);
        this.instanceUuid = new byte[Constants.INSTANCE_ID_BYTE_LENGTH];
        System.arraycopy(instanceUuid, 0, this.instanceUuid, 0, Constants.INSTANCE_ID_BYTE_LENGTH);
      } else if (instanceUuid.length < Constants.INSTANCE_ID_BYTE_LENGTH) {
        LOGGER.warn("The given instance id is less than 36 bytes will append trailing zeros on {}", entityId);
        this.instanceUuid = new byte[Constants.INSTANCE_ID_BYTE_LENGTH];
        System.arraycopy(instanceUuid, 0, this.instanceUuid, 0, instanceUuid.length);
      } else
        this.instanceUuid = instanceUuid;
    }
  }

  public static List<EntityRecord> sortRecordsByOffset(Collection<EntityRecord> records, Dictionary dictionary) {
    Comparator<EntityRecord> compareByLastName = (o1, o2) -> {
      String val1 = dictionary.getValue(o1.getEntityId());
      String val2 = dictionary.getValue(o2.getEntityId());
      if (val1 == null || val2 == null)
        throw new IllegalStateException("One of the entities doe not have have an surrogate id..see " + o1.getEntityId() + " or " + o2.getEntityId());
      return val1.compareTo(val2);
    };
    Comparator<EntityRecord> compareByString = OFFSET_COMPARATOR.thenComparing(compareByLastName);
    return records.stream()
        .sorted(compareByString)
        .collect(Collectors.toList());
  }

  public static List<EntityRecord> sortRecordsByOffset(Collection<EntityRecord> records) {
    return records.stream()
        .sorted(OFFSET_COMPARATOR_BY_ID)
        .collect(Collectors.toList());
  }

  public static List<EntityRecord> sortActiveRecordsByOffset(Collection<EntityRecord> records) {
    return records.stream()
        .filter(eir -> eir.getDeleted() == 0)
        .sorted(Comparator.comparingInt(EntityRecord::getRowGroupOffset))
        .collect(Collectors.toList());
  }

  public int getDecodedLength() {
    return decodedLength;
  }

  public void setDecodedLength(int decodedLength) {
    this.decodedLength = decodedLength;
  }

  public int getNullLength() {
    return nullLength;
  }

  public void setNullLength(int nullLength) {
    this.nullLength = nullLength;
  }

  public String instanceId() {
    return new String(instanceUuid);
  }

  public byte[] getInstanceId() {
    return instanceUuid;
  }

  public void setInstanceId(byte[] instanceUuid) {
    this.instanceUuid = instanceUuid;
  }

  public long getVersion() {
    return version;
  }

  public int totalLength() {
    return valueLength + nullLength;
  }

  public byte getDeleted() {
    return deleted;
  }

  public void setDeleted(byte deleted) {
    this.deleted = deleted;
  }

  public int getValueLength() {
    return valueLength;
  }

  public void setValueLength(int valueLength) {
    this.valueLength = valueLength;
  }

  public int getEntityId() {
    return entityId;
  }

  public int getRowGroupOffset() {
    return rowGroupOffset;
  }

  public void setRowGroupOffset(int rowGroupOffset) {
    this.rowGroupOffset = rowGroupOffset;
  }

  @Override
  public String toString() {
    return "EntityRecord{" +
        "entityId=" + entityId +
        ", rowGroupOffset=" + rowGroupOffset +
        ", valueLength=" + valueLength +
        ", version=" + version +
        ", instanceUuid=" + Arrays.toString(instanceUuid) +
        ", deleted=" + deleted +
        ", nullLength=" + nullLength +
        ", decodedLength=" + decodedLength +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(entityId, rowGroupOffset, valueLength, version, deleted, nullLength, decodedLength, instanceUuid);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other instanceof EntityRecord) {
      EntityRecord otherRecord = (EntityRecord) other;
      return Objects.equals(entityId, otherRecord.entityId) &&
          Objects.equals(rowGroupOffset, otherRecord.rowGroupOffset) &&
          Objects.equals(valueLength, otherRecord.valueLength) &&
          Objects.equals(version, otherRecord.version) &&
          Objects.equals(nullLength, otherRecord.nullLength) &&
          Objects.equals(decodedLength, otherRecord.decodedLength) &&
          Objects.equals(instanceUuid, otherRecord.instanceUuid) &&
          Objects.equals(deleted, otherRecord.deleted);
    }
    return false;
  }
}
