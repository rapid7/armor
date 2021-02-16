package com.rapid7.armor.read.fast;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * A null based armor block reader. This is used to return only null values for a column. This
 * is of use if a new column is introduced and hasn't been populated to all shards yet.
 */
public class FixedValueArmorBlockReader extends FastArmorBlockReader {

  private Object fixedValue;

  public FixedValueArmorBlockReader(Object fixedValue, int numRows) {
    super(null, null, null, numRows, -1, null, null, null);

    this.fixedValue = fixedValue;
  }

  @Override
  public FastArmorBlock getLongBlock(int batchRows) {
    throw new UnsupportedOperationException("Integers are not supported yet.");
  }

  @Override
  public FastArmorBlock getIntegerBlock(int batchRows) {
    throw new UnsupportedOperationException("Integers are not supported yet.");
  }

  @Override
  public FastArmorBlock getStringBlock(int batchRows) {
    if (!(fixedValue instanceof String)) {
      throw new UnsupportedOperationException("The fixed value must be a string.");
    }

    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(Slices.allocate(0), new int[0], null, 0, batchNum);
    }

    int end = Math.min((rowCounterIndex + batchRows), this.numRows);
    int allocate = end - rowCounterIndex;

    int[] sliceOffsets = new int[allocate + 1];
    boolean[] valueIsNull = new boolean[allocate];
    rowCounterIndex += allocate;
    if (rowCounterIndex >= this.numRows)
      hasNext = false;

    String fixedValueString = (String) fixedValue;
    int numBytes = fixedValueString.length() * allocate;
    Slice slice = Slices.allocate(numBytes);

    int fixedValuedLength = fixedValueString.length();
    byte[] fixedValueBytes = fixedValueString.getBytes();
    for (int i = 0; i < allocate; i++) {
      sliceOffsets[i] = i * fixedValuedLength;
      slice.setBytes(sliceOffsets[i], fixedValueBytes);
    }
    sliceOffsets[allocate] = allocate * fixedValuedLength;

    return new FastArmorBlock(slice, sliceOffsets, valueIsNull, allocate, batchNum);
  }

}
