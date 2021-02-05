package com.rapid7.armor.read.fast;


import io.airlift.slice.Slice;

public class FastArmorBlock {
  private Slice slice;
  private long[] longValueArray;
  private int[] intValueArray;
  private int[] offsets;
  private final boolean[] valueIsNull;
  private final int rows;
  private int batchNum = -1;

  public FastArmorBlock(long[] longValueArray, boolean[] valueIsNull, int rows, int batchNum) {
    this.longValueArray = longValueArray;
    this.valueIsNull = valueIsNull;
    this.rows = rows;
    this.batchNum = batchNum;
  }

  public FastArmorBlock(int[] intValueArray, boolean[] valueIsNull, int rows, int batchNum) {
    this.intValueArray = intValueArray;
    this.valueIsNull = valueIsNull;
    this.rows = rows;
    this.batchNum = batchNum;
  }

  public FastArmorBlock(Slice slice, int[] offsets, boolean[] valueIsNull, int rows, int batchNum) {
    this.slice = slice;
    this.offsets = offsets;
    this.valueIsNull = valueIsNull;
    this.rows = rows;
    this.batchNum = batchNum;
  }

  public int getBatchNum() {
    return this.batchNum;
  }

  public int getNumRows() {
    return rows;
  }

  public Slice getSlice() {
    return slice;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public boolean[] getValuesIsNull() {
    return valueIsNull;
  }

  public long[] getLongValueArray() {
    return longValueArray;
  }

  public int[] getIntValueArray() {
    return intValueArray;
  }
}
