package com.rapid7.armor.read.fast;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.Arrays;

/**
 * A null based armor block reader. This is used to return only null values for a column. This
 * is of use if a new column is introduced and hasn't been populated to all shards yet.
 */
public class NullArmorBlockReader extends FastArmorBlockReader {

  public NullArmorBlockReader(int numRows) {
    super(null, null, null, null, numRows, -1, null, null, null);
 }

  public FastArmorBlock getLongBlock(int batchRows) {
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(new long[0], null, 0, batchNum);
    }
    
    int end = Math.min((rowCounterIndex + batchRows), this.numRows);
    int allocate = end - rowCounterIndex;
    
    long[] values = new long[allocate];
    boolean[] valueIsNull = new boolean[batchRows];

    rowCounterIndex += allocate;
    if (rowCounterIndex >= this.numRows)
      hasNext = false;

    Arrays.fill(valueIsNull, true);

    return new FastArmorBlock(values, valueIsNull, allocate, batchNum);
  }
  
  public FastArmorBlock getIntegerBlock(int batchRows) {
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(new long[0], null, 0, batchNum);
    }
    
    int end = Math.min((rowCounterIndex + batchRows), this.numRows);
    int allocate = end - rowCounterIndex;
    
    int[] values = new int[allocate];
    boolean[] valueIsNull = new boolean[allocate];

    rowCounterIndex += allocate;
    if (rowCounterIndex >= this.numRows)
      hasNext = false;

    Arrays.fill(valueIsNull, true);

    return new FastArmorBlock(values, valueIsNull, allocate, batchNum);
  }
  
  public FastArmorBlock getStringBlock(int batchRows) {
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(Slices.allocate(0), new int[0], null, 0, batchNum);
    }
    Slice slice = Slices.allocate(0);
    
    int end = Math.min((rowCounterIndex + batchRows), this.numRows);
    int allocate = end - rowCounterIndex;
    
    int[] sliceOffsets = new int[allocate + 1];
    boolean[] valueIsNull = new boolean[allocate];
    rowCounterIndex += allocate;
    if (rowCounterIndex >= this.numRows)
      hasNext = false;

    Arrays.fill(valueIsNull, true);

    return new FastArmorBlock(slice, sliceOffsets, valueIsNull, allocate, batchNum);
  }

}
