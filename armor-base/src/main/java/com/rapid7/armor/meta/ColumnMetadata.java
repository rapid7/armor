package com.rapid7.armor.meta;


import com.rapid7.armor.schema.DataType;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * Column shard metadata stores various things about the column shard including schmea,
 * stats, endian, version etc.
 */
@JsonInclude(value = Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ColumnMetadata {
  private boolean entityId = false;
  private DataType dataType;
  private String columnName;
  private String lastUpdate;
  private boolean littleEndian = false;
  private int fragmentationLevel = 0;
  private String compressionAlgo = "zstd";
  private Double minValue;
  private Double maxValue;
  private int numRows = 0;
  private int numEntities = 0;
  private int cardinality = 0;
  private String lastCompaction;
  private String lastCompactionDuration;

  public void setEntityId(boolean entityId) {
    this.entityId = entityId;
  }
  
  public boolean getEntityId() {
    return entityId;
  }

  public String getLastCompaction() {
    return lastCompaction;
  }
  
  public String getLastCompactionDuration() {
    return this.lastCompactionDuration;
  }

  public void setLastCompactionDuration(String lastCompactionDuration) {
	this.lastCompactionDuration = lastCompactionDuration;
  }

  public void setLastCompaction(String lastCompaction) {
    this.lastCompaction = lastCompaction;
  }

  public int getCardinality() {
    return cardinality;
  }

  public void setCardinality(int cardinality) {
    this.cardinality = cardinality;
  }

  public int getNumRows() {
    return numRows;
  }

  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  public int getNumEntities() {
    return this.numEntities;
  }

  public void setNumEntities(int numEntities) {
    this.numEntities = numEntities;
  }

  public Double getMinValue() {
    return minValue;
  }

  public void setMinValue(Double minValue) {
    this.minValue = minValue;
  }

  public Double getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(Double maxValue) {
    this.maxValue = maxValue;
  }

  public DataType getColumnType() {
    return dataType;
  }

  public void setColumnType(DataType dataType) {
    this.dataType = dataType;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public String getCompressionAlgorithm() {
    return compressionAlgo;
  }

  public void setCompressionAlgorithm(String compressionAlgo) {
    this.compressionAlgo = compressionAlgo;
  }

  /**
   * @return The date in java.util.Date format of "EEE MMM dd HH:mm:ss zzz yyyy"
   */
  public String getLastUpdate() {
    return lastUpdate;
  }

  public void setLastUpdate(String lastUpdate) {
    this.lastUpdate = lastUpdate;
  }

  public void setApproxFragmentationBytes(long bytes) {
	// Noop don't do anything
  }

  public long getApproxFragmentationBytes() {
	int typeLength;
	if (dataType == null || dataType == DataType.STRING) {
		typeLength = 4;
	} else {
		typeLength = dataType.getByteLength();
	}
	long acutalByteSize = (numRows * typeLength);
	long usedPercentage = 100 - fragmentationLevel;
	  
	return (acutalByteSize * fragmentationLevel) / usedPercentage; 
  }

  public int getFragmentationLevel() {
    return fragmentationLevel;
  }

  public void setFragmentationLevel(int fragmentationLevel) {
    this.fragmentationLevel = fragmentationLevel;
  }

  public boolean getLittleEndian() {
    return littleEndian;
  }

  public void setLittleEndian(boolean littleEndian) {
    this.littleEndian = littleEndian;
  }

  public void handleMinMax(Number value) {
    if (minValue == null)
      minValue = value.doubleValue();
    else {
      if (value.doubleValue() < minValue)
        minValue = value.doubleValue();
    }
    if (maxValue == null)
      maxValue = value.doubleValue();
    else if (value.doubleValue() > maxValue) {
      maxValue = value.doubleValue();
    }
  }

  public void resetMinMax() {
    minValue = null;
    maxValue = null;
  }
}
