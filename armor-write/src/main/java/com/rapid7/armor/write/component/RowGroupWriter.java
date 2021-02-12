package com.rapid7.armor.write.component;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;

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
   * @param consumer The consumer to listen for objects from.
   */
  public void customExtractValues(EntityRecord er, Consumer<List<Object>> consumer) throws IOException {
    long previousPosition = position();
    int currentAlloc = 4096;
    ByteBuffer valBuf = ByteBuffer.allocate(currentAlloc);
    int nilByteAlloc = 4096;
    ByteBuffer nilBuf = ByteBuffer.allocate(nilByteAlloc);
    try {
      int offset = er.getRowGroupOffset();
      position(offset);
      int valueLength = er.getValueLength();
      if (valueLength > currentAlloc) {
        valBuf = ByteBuffer.allocate(valueLength);
        currentAlloc = valueLength;
      } else {
        valBuf.clear();
        valBuf.limit(valueLength);
      }
      int valRead = read(valBuf);
      int nullLength = er.getNullLength();
      final RoaringBitmap nilRb;
      if (nullLength > 0) {
        if (nullLength > nilByteAlloc) {
          nilBuf = ByteBuffer.allocate(nullLength);
          nilByteAlloc = nullLength;
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
      consumer.accept(Arrays.asList(er, values, nilRb));
    } finally {
      position(previousPosition);
    }
  }
  /**
   * Given a list of records traverse through the rowgroup and pass back an array with the ER first, the list of values second
   * and finally the null bit map last.
   * 
   * @param records A list of entity records.
   * @param consumer A consumer to listen for values for each entity.
   */
  public void customTraverseThoughValues(List<EntityRecord> records, Consumer<List<Object>> consumer) throws IOException {
    long previousPosition = position();
    int currentAlloc = 4096;
    ByteBuffer valBuf = ByteBuffer.allocate(currentAlloc);
    int nilByteAlloc = 4096;
    ByteBuffer nilBuf = ByteBuffer.allocate(nilByteAlloc);
    int current = 0;
    try {
      for (EntityRecord er : records) {
        int offset = er.getRowGroupOffset();
        if (current < offset)
          consumer.accept(Arrays.asList(true, current, offset));
        position(offset);
        int valueLength = er.getValueLength();
        if (valueLength > currentAlloc) {
          valBuf = ByteBuffer.allocate(valueLength);
          currentAlloc = valueLength;
        } else {
          valBuf.clear();
          valBuf.limit(valueLength);
        }

        int valRead = read(valBuf);
        current += valRead;
        int nullLength = er.getNullLength();
        final RoaringBitmap nilRb;
        if (nullLength > 0) {
          if (nullLength > nilByteAlloc) {
            nilBuf = ByteBuffer.allocate(nullLength);
            nilByteAlloc = nullLength;
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
      position(previousPosition);
    }
  }

  /**
   * Does a pass through the row group to update statistics and bitmap.
   * 
   * @param metadata The metadata of the columnfile.
   * @param records A list of records to traverse through.
   */
  public void runThoughValues(ColumnMetadata metadata, List<EntityRecord> records) throws IOException {
    long previousPosition = position();
    Double prevMax = metadata.getMaxValue();
    Double prevMin = metadata.getMinValue();

    int currentAlloc = 4096;
    ByteBuffer valBuf = ByteBuffer.allocate(currentAlloc);
    int nilByteAlloc = 4096;
    ByteBuffer nilBuf = ByteBuffer.allocate(nilByteAlloc);
    Set<Object> cardinality = new HashSet<>();
    boolean success = false;
    try {
      metadata.resetMinMax();
      for (EntityRecord eir : records) {
        if (eir.getDeleted() == 1)
          continue;
        int offset = eir.getRowGroupOffset();
        position(offset);
        int valueLength = eir.getValueLength();
        if (valueLength > currentAlloc) {
          valBuf = ByteBuffer.allocate(valueLength);
          currentAlloc = valueLength;
        } else {
          valBuf.clear();
          valBuf.limit(valueLength);
        }

        int valRead = read(valBuf);
        int nullLength = eir.getNullLength();
        final RoaringBitmap nilRb;
        if (nullLength > 0) {
          if (nullLength > nilByteAlloc) {
            nilBuf = ByteBuffer.allocate(nullLength);
            nilByteAlloc = nullLength;
          } else {
            nilBuf.clear();
            nilBuf.limit(nullLength);
          }
          int nilRead = read(nilBuf);
          nilBuf.flip();
          nilRb = new RoaringBitmap();
          nilRb.deserialize(nilBuf);
        } else
          nilRb = null;

        dataType.traverseByteBuffer(valBuf, nilRb, eir.getValueLength(), (rowCount, value) -> {
          if (nilRb == null || !nilRb.contains(rowCount)) {
            metadata.handleMinMax(value);
            cardinality.add(value);
          }
        });
      }
      metadata.setCardinality(cardinality.size());
      success = true;
    } finally {
      if (!success) {
        metadata.setMaxValue(prevMax);
        metadata.setMinValue(prevMin);
      }
      position(previousPosition);
    }
  }

  /**
   * Appends the values for multiple entities.
   *
   * @param valueArray A list of object array representing the values.
   */
  public List<RgOffsetWriteResult> appendEntityValues(List<Object[]> valueArray) throws IOException {
    int totalNumRows = valueArray.stream().mapToInt(l -> l.length).sum();
    int totalRequiredBytes = dataType.determineByteLength(totalNumRows) * 2; // Start off by doubling the bytebuffer alloc.
    long beforeAppendPosition = position();
    boolean reallocaed = false;
    try {
      ByteBuffer output = ByteBuffer.allocate(totalRequiredBytes);
      List<RgOffsetWriteResult> positions = new ArrayList<>();
      RgOffsetWriteResult previous = null;
      for (Object[] values : valueArray) {
        RgOffsetWriteResult offsetResult = new RgOffsetWriteResult();
        if (previous != null) {
          if (previous.valueLength > 0) {
            long testPosition = beforeAppendPosition + output.position();
            if (testPosition <= previous.rowGroupOffset) {
              LOGGER.error("Preiouvs offset is {}", offsetResult);
              LOGGER.error("Before possition {}", beforeAppendPosition);
              LOGGER.error("Output position {}", output.position());
              LOGGER.error("Reallocat {}", reallocaed);
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
      write(output); // NOTE: This approach has a max of Integer.MAX bytes due to bytebuffer limitation.
      return positions;
    } catch (OutOfMemoryError e) {
      LOGGER.error("Unable to allocate {} bytes", totalRequiredBytes);
      throw e;
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
   */
  public List<EntityRecord> compact(List<EntityRecord> entitiesToKeep) throws IOException {
    int totalRequiredBytes = entitiesToKeep.stream().mapToInt(EntityRecord::totalLength).sum();
    ByteBuffer output = ByteBuffer.allocate(totalRequiredBytes * 2);
    Path path = Files.createTempFile(columnShardId.alternateString() + ROWGROUP_COMPACTION_SUFFIX, ".armor");
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

          // Restrict the read to expected length
          output.limit(output.position() + er.totalLength());
          int read = read(output);

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
