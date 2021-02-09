package com.rapid7.armor.write.component;

public class EntityIndexVariableWidthException extends RuntimeException {
  private static final long serialVersionUID = 547001677457306098L;
  private int recordSize;
  private int size;
  private int difference;
  private int startSize;
  private String columnShardInfo;
  
  public EntityIndexVariableWidthException(int recordSize, int size, int difference, int startSize, String columnShardInfo) {
    this.recordSize = recordSize;
    this.size = size;
    this.difference = difference;
    this.startSize = startSize;
    this.columnShardInfo = columnShardInfo;
  }
  
  @Override
  public String getMessage() {
    return "The entity index size " + size + " is not in expected fixed width of " + recordSize + ". It is " + difference + " bytes off. Started size was " +
       startSize + ". Go check out " + columnShardInfo;
  }
}
