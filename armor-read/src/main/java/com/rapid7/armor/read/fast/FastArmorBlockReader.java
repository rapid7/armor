package com.rapid7.armor.read.fast;

import java.nio.ByteBuffer;

import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.DictionaryReader;
import com.rapid7.armor.schema.DataType;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * Specialized reader that tracks what is currently read to the entity structure.
 */
public class FastArmorBlockReader {
  private final ByteBuffer columnValues;
  protected boolean hasNext = true;
  protected final int numRows;
  private final int[] entityDecodedLength;
  private final int[] entityNumRows;
  private int entityCounter;
  private final DictionaryReader strValueDictionary;
  private final int numEntities;
  protected int rowCounterIndex;
  private final IntArrayList rowIsNull;
  protected int batchNum = 0;
  private DataType dataType;
  private ColumnMetadata metadata;

  public FastArmorBlockReader(
    ColumnMetadata metadata,
    ByteBuffer columnValues,
    IntArrayList rowIsNull,
    DictionaryReader strValueDictionary,
    int numRows,
    int numEntities,
    int[] entityDecodedLength,
    int[] entityNumRows,
    DataType dataType) {
    this.metadata = metadata;
    this.columnValues = columnValues;
    this.strValueDictionary = strValueDictionary;
    this.numRows = numRows;
    this.numEntities = numEntities;
    this.entityDecodedLength = entityDecodedLength;
    this.entityNumRows = entityNumRows;
    this.entityCounter = 0;
    this.rowCounterIndex = 0;
    this.rowIsNull = rowIsNull; // Note zero-indexed row number
    this.dataType = dataType;
  }

  public ColumnMetadata metadata() {
    return metadata;    
  }
  public DataType dataType() {
    return dataType;
  }

  public int rowCounter() {
    return rowCounterIndex;
  }

  public int entityCounter() {
    return entityCounter;
  }

  public int numRows() {
    return numRows;
  }

  public int batchNum() {
    return batchNum;
  }

  public boolean hasNext() {
    return hasNext;
  }
  
  public DictionaryReader valueDictionary() {
    return strValueDictionary;
  }

  public int nextBatchSize(int desiredBatch) {
    int remaining = numRows - rowCounterIndex;
    if (remaining > desiredBatch)
      return desiredBatch;
    else
      return remaining;
  }

  public FastArmorBlock getLongBlock(int batchRows) {
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(new long[0], null, 0, batchNum);
    }
    // Long blocks we dont need to do any slicing by entities, rather we can
    // pass a page of longs at at time.

    long[] values = new long[batchRows];
    // Loop unroll for faster processing in increments of 10
    int rowsRead = 0;
    int rowReadStartIndex = rowCounterIndex;
    for (; rowCounterIndex < this.numRows; rowCounterIndex += 10) {
      if (rowsRead + 10 > this.numRows || rowsRead + 10 > batchRows)
        break;
      values[rowsRead] = columnValues.getLong();
      values[rowsRead + 1] = columnValues.getLong();
      values[rowsRead + 2] = columnValues.getLong();
      values[rowsRead + 3] = columnValues.getLong();
      values[rowsRead + 4] = columnValues.getLong();
      values[rowsRead + 5] = columnValues.getLong();
      values[rowsRead + 6] = columnValues.getLong();
      values[rowsRead + 7] = columnValues.getLong();
      values[rowsRead + 8] = columnValues.getLong();
      values[rowsRead + 9] = columnValues.getLong();
      rowsRead += 10;
    }

    for (; rowCounterIndex < this.numRows; rowCounterIndex++) {
      if (rowsRead >= batchRows)
        break;
      values[rowsRead] = columnValues.getLong();
      rowsRead++;
    }

    if (rowCounterIndex >= this.numRows)
      hasNext = false;
    if (!rowIsNull.isEmpty()) {
      boolean[] valueIsNull = new boolean[rowCounterIndex - rowReadStartIndex];
      for (int nullRow : rowIsNull) {
        int nullRowIndex = nullRow - 1;
        if (nullRowIndex >= rowReadStartIndex && nullRowIndex < rowCounterIndex)
          valueIsNull[nullRowIndex - rowReadStartIndex] = true;
      }

      return new FastArmorBlock(values, valueIsNull, rowsRead, batchNum);
    } else {
      return new FastArmorBlock(values, null, rowsRead, batchNum);
    }
  }

  public FastArmorBlock getIntegerBlock(int batchRows) {
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(new int[0], null, 0, batchNum);
    }
    // Integer blocks we dont need to do any slicing by entities, rather we can
    // pass a page of ints at at time.

    int[] values = new int[batchRows];
    // Loop unroll for faster processing in increments of 10
    int rowsRead = 0;
    int rowReadStartIndex = rowCounterIndex;
    for (; rowCounterIndex < this.numRows; rowCounterIndex += 10) {
      if (rowsRead + 10 > this.numRows || rowsRead + 10 > batchRows)
        break;
      values[rowsRead] = columnValues.getInt();
      values[rowsRead + 1] = columnValues.getInt();
      values[rowsRead + 2] = columnValues.getInt();
      values[rowsRead + 3] = columnValues.getInt();
      values[rowsRead + 4] = columnValues.getInt();
      values[rowsRead + 5] = columnValues.getInt();
      values[rowsRead + 6] = columnValues.getInt();
      values[rowsRead + 7] = columnValues.getInt();
      values[rowsRead + 8] = columnValues.getInt();
      values[rowsRead + 9] = columnValues.getInt();
      rowsRead += 10;
    }

    for (; rowCounterIndex < this.numRows; rowCounterIndex++) {
      if (rowsRead >= batchRows)
        break;
      values[rowsRead] = columnValues.getInt();
      rowsRead++;
    }

    if (rowCounterIndex >= this.numRows)
      hasNext = false;
    if (!rowIsNull.isEmpty()) {
      boolean[] valueIsNull = new boolean[rowCounterIndex - rowReadStartIndex];
      for (int nullRow : rowIsNull) {
        int nullRowIndex = nullRow - 1;
        if (nullRowIndex >= rowReadStartIndex && nullRowIndex < rowCounterIndex)
          valueIsNull[nullRowIndex - rowReadStartIndex] = true;
      }

      return new FastArmorBlock(values, valueIsNull, rowsRead, batchNum);
    } else {
      return new FastArmorBlock(values, null, rowsRead, batchNum);
    }
  }

  public FastArmorBlock getStringBlock(int batchRows) {
    if (strValueDictionary == null)
      throw new IllegalStateException("No dictionary was setup for reading string blocks");
    batchNum++;
    if (numRows == 0) {
      return new FastArmorBlock(Slices.allocate(0), new int[0], null, 0, batchNum);
    }
    // Calculate how much memory to allocate
    int allocRowCount = 0;
    int allocSize = 0;
    int stopAtEntity = entityCounter;

    int entityRowOffset = calculateEntityRowOffset(entityCounter, rowCounterIndex);
    int initialRows = entityNumRows[entityCounter];
    int ls = initialRows - entityRowOffset;

    for (; stopAtEntity < numEntities; stopAtEntity++) {
      if (allocRowCount >= batchRows) {
        break;
      }
      if (stopAtEntity == entityCounter)
        allocRowCount += ls;
      else
        allocRowCount += entityNumRows[stopAtEntity];
      allocSize += entityDecodedLength[stopAtEntity];
    }

    // Determine where we will stop given the batch rows etc.

    int sliceIndex = 0;
    IntArrayList sliceOffsets = new IntArrayList();
    sliceOffsets.add(sliceIndex);
    Slice slice = Slices.allocate(allocSize);
    BooleanArrayList valueIsNull = new BooleanArrayList(); // Row index based
    int sessionReadRows = 0;

    // Important, we need precalculate the entity row offset, which will be a looking at the
    // rowcounter and the the entity counter to find the.
    for (; entityCounter < stopAtEntity; entityCounter++) {
      int rowsToRead = entityNumRows[entityCounter];
      //System.out.println(entityRowOffset +  " " + entityCounter + " " + rowsToRead + " " + sessionReadRows);
      for (; entityRowOffset < rowsToRead; entityRowOffset++) {
        if (sessionReadRows >= batchRows) {
          hasNext = rowCounterIndex < numRows;
          sliceOffsets.trim();
          valueIsNull.trim();
          return new FastArmorBlock(slice, sliceOffsets.elements(), valueIsNull.elements(), sessionReadRows, batchNum);
        }
        byte[] bytes = strValueDictionary.getValue(columnValues.getInt());
        slice.setBytes(sliceIndex, bytes, 0, bytes.length);
        sliceIndex += bytes.length;
        sliceOffsets.add(sliceIndex);
        valueIsNull.add(bytes.length == 0);
        rowCounterIndex++;
        sessionReadRows++;
      }
      if (sessionReadRows >= batchRows) {
        hasNext = rowCounterIndex < numRows;
        sliceOffsets.trim();
        valueIsNull.trim();
        entityCounter++;
        return new FastArmorBlock(slice, sliceOffsets.elements(), valueIsNull.elements(), sessionReadRows, batchNum);
      }
      entityRowOffset = 0; // Any further offsets will be reset to zero.
    }

    if (sessionReadRows < batchRows && entityCounter < this.numEntities) {
      entityCounter++;
      entityRowOffset = 0;
      int rowsToRead = entityNumRows[entityCounter];
      for (; entityRowOffset < rowsToRead; entityRowOffset++) {
        if (sessionReadRows >= batchRows) {
          hasNext = rowCounterIndex < numRows;
          sliceOffsets.trim();
          valueIsNull.trim();
          return new FastArmorBlock(slice, sliceOffsets.elements(), valueIsNull.elements(), sessionReadRows, batchNum);
        }
        byte[] bytes = strValueDictionary.getValue(columnValues.getInt());
        slice.setBytes(sliceIndex, bytes, 0, bytes.length);
        sliceIndex += bytes.length;
        sliceOffsets.add(sliceIndex);
        valueIsNull.add(bytes.length == 0);
        rowCounterIndex++;
        sessionReadRows++;
      }
      //System.out.println(sessionReadRows);
      if (sessionReadRows >= batchRows) {
        hasNext = rowCounterIndex < numRows;
        sliceOffsets.trim();
        valueIsNull.trim();
        return new FastArmorBlock(slice, sliceOffsets.elements(), valueIsNull.elements(), sessionReadRows, batchNum);
      }
    }
    hasNext = rowCounterIndex < numRows;
    sliceOffsets.trim();
    valueIsNull.trim();
    return new FastArmorBlock(slice, sliceOffsets.elements(), valueIsNull.elements(), sessionReadRows, batchNum);
  }

  private int calculateEntityRowOffset(int entityCounter, int rowCounterIndex) {
    // Read up to the entity counter - 1
    int rowsRead = 0;
    int stopAt = entityCounter - 1;
    for (int i = 0; i <= stopAt; i++) {
      rowsRead += entityNumRows[i];
    }

    // We are now at the current entity, so offset should now be rowCounter - rowsRead
    return rowCounterIndex - rowsRead;
  }
}
