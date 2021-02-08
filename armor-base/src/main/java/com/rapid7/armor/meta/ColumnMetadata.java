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
  private int format = 1;
  private DataType dataType;
  private String columnId;
  private String lastUpdate;
  private boolean littleEndian = false;
  private int fragmentationLevel = 0;
  private String compressionAlgo = "zstd";
  private Double minValue;
  private Double maxValue;
  private int numRows = 0;
  private int numEntities = 0;
  private int cardinaility = 0;
  private String lastDefrag;

  public String getLastDefrag() {
    return lastDefrag;
  }

  public void setLastDefrag(String lastDefrag) {
    this.lastDefrag = lastDefrag;
  }

  public int getCardinality() {
    return cardinaility;
  }

  public void setCardinality(int cardinality) {
    this.cardinaility = cardinality;
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

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public int getFormatVersion() {
    return format;
  }

  public void setFormatVersion(int version) {
    this.format = version;
  }

  public String getColumnId() {
    return columnId;
  }

  public void setColumnId(String columnId) {
    this.columnId = columnId;
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
