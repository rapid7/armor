package com.rapid7.armor.write.component;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.FixedCapacityByteBufferPool;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.write.writers.TempFileUtil;

import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RowGroupWriter that writes directly to underlying channel.
 */
public class RowGroupWriter extends FileComponent {
  private static final Logger LOGGER = LoggerFactory.getLogger(RowGroupWriter.class);
  private final ColumnShardId columnShardId;
  private final DictionaryWriter dictionaryWriter;
  private final DataType dataType;
  private final static String ROWGROUP_COMPACTION_SUFFIX = "_rowgroup_compaction-";
  private final static int DEFAULT_BYTEBUFFER_SIZE = 128000;
  private static FixedCapacityByteBufferPool BYTE_BUFFER_POOL = new FixedCapacityByteBufferPool(DEFAULT_BYTEBUFFER_SIZE);

  /**
   * Readjusts the fixed capacity size, this is to be primarily used for testing.
   *
   * @param fixedSize The size in bytes to setup the bytebuffer pool.
   */
  public static void setupFixedCapacityBufferPoolSize(int fixedSize) {
    BYTE_BUFFER_POOL = new FixedCapacityByteBufferPool(fixedSize);
  }

  public RowGroupWriter(Path path, ColumnShardId columnShardId, DictionaryWriter dictionary) {
    super(path);
    this.dictionaryWriter = dictionary;
    this.columnShardId = columnShardId;
    this.dataType = columnShardId.getColumnId().dataType();
  }
  
  public DictionaryWriter getDictionaryWriter() {
    return dictionaryWriter;
  }
  
  /**
   * Given a record, go and extract the values for the given record.
   *
   * @param er The entity record to extract values from.
   * 
   * @return The list of values for that entity.
   * 
   * @throws IOException If an IO error occurs.
   */
  public List<Object> getEntityValues(EntityRecord er) throws IOException {
    long previousPosition = position();
    ByteBuffer valBorrow = BYTE_BUFFER_POOL.get();
    ByteBuffer nilBorrow = BYTE_BUFFER_POOL.get();
    try {
      ByteBuffer valBuf = valBorrow;
      ByteBuffer nilBuf = nilBorrow;
      int offset = er.getRowGroupOffset();
      position(offset);
      int valueLength = er.getValueLength();
      if (valueLength > valBuf.capacity()) {
        valBuf = ByteBuffer.allocate(valueLength);
      } else {
        valBuf.clear();
        valBuf.limit(valueLength);
      }
      int valRead = read(valBuf);
      int nullLength = er.getNullLength();
      final RoaringBitmap nilRb;
      if (nullLength > 0) {
        if (nullLength > nilBuf.capacity()) {
          nilBuf = ByteBuffer.allocate(nullLength);
        } else {
          nilBuf.clear();
          nilBuf.limit(nullLength);
        }
        read(nilBuf);
        nilBuf.flip();
        nilRb = new RoaringBitmap();
        nilRb.deserialize(nilBuf);
      } else
        nilRb = null;
      List<Object> values = dataType.traverseByteBufferToList(valBuf, er.getValueLength());
      if (nilRb != null) {
        for (int rowNum : nilRb.toArray()) {
          values.set(rowNum-1, null);
        }
      }
      return values;
    } finally {
      BYTE_BUFFER_POOL.release(valBorrow);
      BYTE_BUFFER_POOL.release(nilBorrow);
      position(previousPosition);
    }
  }

  /**
   * Given a list of records traverse through the rowgroup and pass back an array with the ER first, the list of values second
   * and finally the null bit map last.
   * 
   * @param records A list of entity records.
   * @param consumer A consumer to listen for values for each entity.
   * 
   * @throws IOException If an IO error occurs.
   */
  public void customTraverseThoughValues(List<EntityRecord> records, Consumer<List<Object>> consumer) throws IOException {
    long previousPosition = position();
    int current = 0;
    ByteBuffer valBorrow = BYTE_BUFFER_POOL.get();
    ByteBuffer nilBorrow = BYTE_BUFFER_POOL.get();
    try {
      ByteBuffer valBuf = valBorrow;
      ByteBuffer nilBuf = nilBorrow;

      for (EntityRecord er : records) {
        int offset = er.getRowGroupOffset();
        if (current < offset)
          consumer.accept(Arrays.asList(true, current, offset));
        position(offset);
        int valueLength = er.getValueLength();
        if (valueLength > valBuf.capacity()) {
          valBuf = ByteBuffer.allocate(valueLength);
        } else {
          valBuf.clear();
          valBuf.limit(valueLength);
        }

        int valRead = read(valBuf);
        current += valRead;
        int nullLength = er.getNullLength();
        final RoaringBitmap nilRb;
        if (nullLength > 0) {
          if (nullLength > nilBuf.capacity()) {
            nilBuf = ByteBuffer.allocate(nullLength);
          } else {
            nilBuf.clear();
            nilBuf.limit(nullLength);
          }
          int nilRead = read(nilBuf);
          current += nilRead;
          nilBuf.flip();
          nilRb = new RoaringBitmap();
          nilRb.deserialize(nilBuf);
        } else
          nilRb = null;

        List<Object> values = dataType.traverseByteBufferToList(valBuf, er.getValueLength());
        consumer.accept(Arrays.asList(false, er, values, nilRb));
      }
    } finally {
      BYTE_BUFFER_POOL.release(valBorrow);
      BYTE_BUFFER_POOL.release(nilBorrow);
      position(previousPosition);
    }
  }

  private class MetadataUpdater {
    private final ColumnMetadata metadata;
    Double prevMax;
    Double prevMin;
    Set<Object> cardinality = new HashSet<>();
    ByteBuffer valBorrow = BYTE_BUFFER_POOL.get();
    ByteBuffer nilBorrow = BYTE_BUFFER_POOL.get();
    ByteBuffer valBuf = valBorrow;
    ByteBuffer nilBuf = nilBorrow;
    private boolean success;


    MetadataUpdater(ColumnMetadata metadata) {
      this.metadata = metadata;
      prevMax = metadata.getMaxValue();
      prevMax = metadata.getMinValue();

      metadata.resetMinMax();
      success = false;
    }

    void resetOldValues() {
      metadata.setMaxValue(prevMax);
      metadata.setMinValue(prevMin);
    }

    void cleanup() {
      BYTE_BUFFER_POOL.release(valBorrow);
      BYTE_BUFFER_POOL.release(nilBorrow);
      if (!success) {
        resetOldValues();
      }
    }

    public void updateRow(EntityRecord eir) throws IOException {
      int offset = eir.getRowGroupOffset();
      position(offset);
      int valueLength = eir.getValueLength();
      valBuf = insureByteBufferIsBigEnough(valueLength, valBuf);
      int valRead = read(valBuf);
      int nullLength = eir.getNullLength();
      final RoaringBitmap nilRb;
      nilBuf = insureByteBufferIsBigEnough(nullLength, nilBuf);
      if (nullLength > 0) {
        int nilRead = read(nilBuf);
        nilBuf.flip();
        nilRb = new RoaringBitmap();
        nilRb.deserialize(nilBuf);
      } else {
        nilRb = null;
        nilBuf.flip();
      }

      dataType.traverseByteBuffer(valBuf, nilRb, eir.getValueLength(), (rowCount, value) -> {
        if (nilRb == null || !nilRb.contains(rowCount)) {
          metadata.handleMinMax(value);
          cardinality.add(value);
        }
      });
    }

    public void writeBuffersToOutput(ByteBuffer bb, int totalLength) {

      valBuf.rewind();
      nilBuf.rewind();
      //valBuf.limit();

      bb.put(valBuf);
      bb.put(nilBuf);
    }

    private ByteBuffer insureByteBufferIsBigEnough(int len, ByteBuffer buf) {
      if (len > buf.capacity()) {
        buf = ByteBuffer.allocate(len);
      } else {
        buf.clear();
        buf.limit(len);
      }
      return buf;
    }

    public void finishUpdate() {
      metadata.setCardinality(cardinality.size());
      success = true;
    }
  }


  /**
   * Does a pass through the row group to update statistics and bitmap.
   * 
   * @param metadata The metadata of the columnfile.
   * @param records A list of records to traverse through.
   * 
   * @throws IOException If an io error occurs.
   */

  // Both runThoughValues() and compactAndUpdateMetadata() share the same logic to update metadata.
  // 1 - during compactAndUpdateMetadata(), the entities are in the process of being rewritten - will that affect the ability to do some of the position() stuff that is being done?
  // 2 - there is some try/finally stuff which also needs to be managed. in EntityIndexWriter, we did the inversion from a for loop to being a callback that is callable
  //    from within the for loop.
  public void runThoughValues(ColumnMetadata metadata, List<EntityRecord> records) throws IOException {
    long previousPosition = position();
    MetadataUpdater metadataUpdater = new MetadataUpdater(metadata);
    try {
      for (EntityRecord eir : records) {
        if (eir.getDeleted() == 1)
          continue;
        metadataUpdater.updateRow(eir);
      }
      metadataUpdater.finishUpdate();
    } finally {
      metadataUpdater.cleanup();
      position(previousPosition);
    }
  }

  /**
   * Appends the values for multiple entities.
   *
   * @param valueArray A list of object array representing the values.
   * 
   * @return A list of offset write results.
   * 
   * @throws IOException If an io error occurs.
   */
  public List<RgOffsetWriteResult> appendEntityValues(List<Object[]> valueArray) throws IOException {
    int totalNumRows = valueArray.stream().mapToInt(l -> l.length).sum();
    int totalRequiredBytes = dataType.determineByteLength(totalNumRows) * 2; // Start off by doubling the bytebuffer alloc.
    long beforeAppendPosition = position();
    boolean reallocaed = false;
    ByteBuffer outBorrow = BYTE_BUFFER_POOL.get();
    try {
      ByteBuffer output = null;
      if (outBorrow.capacity() < totalRequiredBytes)
        output = ByteBuffer.allocate(totalRequiredBytes);
      else {
        output = outBorrow;
      }
      List<RgOffsetWriteResult> positions = new ArrayList<>();
      RgOffsetWriteResult previous = null;
      for (Object[] values : valueArray) {
        RgOffsetWriteResult offsetResult = new RgOffsetWriteResult();
        if (previous != null) {
          if (previous.valueLength > 0) {
            long testPosition = beforeAppendPosition + output.position();
            if (testPosition <= previous.rowGroupOffset) {
              // If it falls into this error here, then we have an offset error. Print out the information for debugging.
              LOGGER.error("Preiouvs offset is {}", offsetResult);
              LOGGER.error("Before possition {}", beforeAppendPosition);
              LOGGER.error("Output position {}", output.position());
              LOGGER.error("Reallocated {}", reallocaed);
              throw new RuntimeException("ERROR!!!!!!");
            }
          }
        }
        offsetResult.rowGroupOffset = beforeAppendPosition + output.position();
        Object[] valueRequest = values;
        int strLength = 0;
        if (dataType == DataType.STRING) {
          IntArrayList surrogates = new IntArrayList();
          for (Object v : values) {
            if (v == null) {
              surrogates.add(0);
            } else {
              String strValue = (String) v;
              strLength += strValue.getBytes().length; // Special characters mask the String.length method where its not included in size.
              Integer surrogate = dictionaryWriter.getSurrogate(strValue);
              surrogates.add(surrogate.intValue());
            }
          }
          valueRequest = surrogates.toArray();
        }
        HashSet<Integer> nullPositions = new HashSet<>();

        long requiredCapacity2 = output.position() + dataType.determineByteLength(valueRequest.length);
        if (requiredCapacity2 > output.capacity()) {
          reallocaed = true;
          ByteBuffer temp = ByteBuffer.allocate((int) requiredCapacity2 * 2);
          output.flip();
          temp.put(output);
          output.clear();
          output = temp;
        }

        offsetResult.valueLength = dataType.writeValuesToByteBuffer(output, nullPositions, valueRequest);
        if (dataType == DataType.STRING)
          offsetResult.decodedLength = strLength;
        else
          offsetResult.decodedLength = offsetResult.valueLength;

        RoaringBitmap rb = writeNullPositions(nullPositions, output);
        if (rb == null)
          offsetResult.nullLength = 0;
        else {
          offsetResult.nullLength = rb.serializedSizeInBytes();
          long requiredCapacity = output.position() + offsetResult.valueLength + offsetResult.nullLength;
          if (output.capacity() < requiredCapacity) {
            reallocaed = true;
            ByteBuffer temp = ByteBuffer.allocate((int) requiredCapacity * 2);
            output.flip();
            temp.put(output);
            output.clear();
            output = temp;
          }
          try {
            rb.serialize(output);
          } catch (BufferOverflowException boe) {
            LOGGER.error("Detected a buffer overflow, here are the stats..buffer capacity {}, required capacity {}", output.capacity(), requiredCapacity);
            LOGGER.error("null length {}, valueLength {} buffer current position {}", offsetResult.nullLength, offsetResult.valueLength, output.position());
            throw boe;
          }
        }

        previous = offsetResult;
        positions.add(offsetResult);
      }
      output.flip();
      int written = write(output); // NOTE: This approach has a max of Integer.MAX bytes due to bytebuffer limitation.
      return positions;
    } catch (OutOfMemoryError e) {
      LOGGER.error("Unable to allocate {} bytes", totalRequiredBytes);
      throw e;
    } finally {
      BYTE_BUFFER_POOL.release(outBorrow);
    }
  }

  private RoaringBitmap writeNullPositions(Set<Integer> nullPositions, ByteBuffer buffer) {
    if (!nullPositions.isEmpty()) {
      RoaringBitmap bitmap = RoaringBitmap.bitmapOf(nullPositions.stream().mapToInt(i -> i).toArray());
      bitmap.runOptimize();
      return bitmap;
    }
    return null;
  }

  private RgOffsetWriteResult fillNullValues(ByteBuffer bytesBuffer, int numNull) {
    RgOffsetWriteResult offsetResult = new RgOffsetWriteResult();
    offsetResult.rowGroupOffset = bytesBuffer.position();
    HashSet<Integer> nullPositions = new HashSet<>();
    offsetResult.valueLength = dataType.writeValuesToByteBuffer(bytesBuffer, nullPositions, new Object[numNull]);
    if (dataType == DataType.STRING)
      offsetResult.decodedLength = 0;
    else
      offsetResult.decodedLength = offsetResult.valueLength;

    RoaringBitmap rb = writeNullPositions(nullPositions, bytesBuffer);
    if (rb == null)
      offsetResult.nullLength = 0;
    else {
      offsetResult.nullLength = rb.serializedSizeInBytes();
      long requiredCapacity = offsetResult.valueLength + offsetResult.nullLength;
      if (bytesBuffer.capacity() < requiredCapacity) {
        ByteBuffer temp = ByteBuffer.allocate((int) requiredCapacity);
        bytesBuffer.flip();
        temp.put(bytesBuffer);
        bytesBuffer.clear();
        bytesBuffer = temp;
      }
      rb.serialize(bytesBuffer);
    }
    return offsetResult;
  }

  /**
   * Compacts the row group given a set of entity records to keep, this will rebuild the underlying
   * group shifting certain portions of data from one place to another. Compaction is done as a one
   * scan pass where it copies contents from one file to another.
   * 
   * @param entitiesToKeep A list of records to keep while compacting.
   * 
   * @return A list of entity records.
   * 
   * @throws IOException If an io error occurs.
   */
  public List<EntityRecord> compact(List<EntityRecord> entitiesToKeep) throws IOException {
    return compactAndUpdateMetadata(entitiesToKeep, null);
  }

  public List<EntityRecord> compactAndUpdateMetadata(List<EntityRecord> entitiesToKeep, ColumnMetadata metadata) throws IOException {
    MetadataUpdater metadataUpdater = null;
    if (metadata != null) {
      metadataUpdater = new MetadataUpdater(metadata);
    }
    int totalRequiredBytes = entitiesToKeep.stream().mapToInt(EntityRecord::totalLength).sum();
    ByteBuffer output = ByteBuffer.allocate(totalRequiredBytes * 2);
    Path path = TempFileUtil.createTempFile(columnShardId.alternateString() + ROWGROUP_COMPACTION_SUFFIX, ".armor");
    boolean copied = false;
    try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      for (EntityRecord er : entitiesToKeep) {
        if (er.getDeleted() == 1)
          continue;
        long position = output.position();
        // If the offset is less than zero, then this is a filler to introduce null rows for certain entities. Fill it in.
        if (er.getRowGroupOffset() == Constants.NULL_FILLER_INDICATOR) {
          // The value length should tell us how many null values to put in.
          int numNullValues = dataType.determineNumValues(er.getValueLength());
          RgOffsetWriteResult result = fillNullValues(output, numNullValues);
          er.setNullLength((int) result.nullLength);
          er.setValueLength((int) result.valueLength);
          er.setDecodedLength((int) result.decodedLength);
        } else {
          // Move the position into place and read contents from file
          position(er.getRowGroupOffset());

          // metadataUpdater.updateRow / writeBuffersToOutput reads values and nulls separately,
          // so that the work in runThroughValues can be done here as well.
          if (metadataUpdater != null) {
            output.limit(output.position() + er.totalLength());
            metadataUpdater.updateRow(er);
            metadataUpdater.writeBuffersToOutput(output, er.totalLength()); // will write values and nulls to output buffer
          } else {
            // Restrict the read to expected length
            output.limit(output.position() + er.totalLength());
            int read = read(output);
          }
          // Expand to full capacity
          output.limit(output.capacity());
        }
        er.setRowGroupOffset((int) position);
      }

      // Now write to the file
      output.flip();
      fileChannel.write(output);
      copied = true;
    } finally {
      if (!copied)
        Files.deleteIfExists(path);
    }
    // Almost done, now do a replace.
    boolean rebased = false;
    try {
      rebase(path);
      rebased = true;
    } finally {
      if (!rebased)
        Files.deleteIfExists(path);
    }
    return entitiesToKeep;
  }

  public static class RgOffsetWriteResult {
    public long rowGroupOffset;
    public long valueLength;
    public long nullLength;
    public long decodedLength;

    @Override
    public String toString() {
      return "offset:" + rowGroupOffset + ",valueLength:" + valueLength + " nullLength: " + nullLength + " decodedLength: " + decodedLength;
    }
  }
}
