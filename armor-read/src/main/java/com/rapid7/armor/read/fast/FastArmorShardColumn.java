package com.rapid7.armor.read.fast;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;

import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdInputStream;
import com.rapid7.armor.columnfile.ColumnFileSection;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.BaseArmorShardColumn;
import com.rapid7.armor.schema.DataType;

import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * An armor shard version that tries to provide the column data as fast as possible.
 * <p>
 * 1) Avoid returning objects.
 * 2) Avoid unnecssary alloc/dealloc
 * 3) Avoid unnessary looping
 */
public class FastArmorShardColumn extends BaseArmorShardColumn {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastArmorShardColumn.class);
  private ByteBuffer columnValues;
  private int[] entityNumRows;
  private int[] entityDecodedLength;
  private IntArrayList rowsIsNull; // Row number that is null (NOTE: NOT zero-indexed)

  public FastArmorShardColumn(InputStream inputStream) throws IOException {
    try {
      load(new DataInputStream(inputStream));
    } finally {
      inputStream.close();
    }
  }

  public ColumnMetadata getMetadata() {
    return metadata;
  }

  public DataType getDataType() {
    return metadata.getDataType();
  }

  public String columnId() {
    return metadata.getColumnId();
  }

  public FastArmorBlockReader getFastArmorColumnReader() {
    return new FastArmorBlockReader(
        columnValues,
        rowsIsNull,
        strValueDictionary,
        metadata.getNumRows(),
        metadata.getNumEntities(),
        entityDecodedLength,
        entityNumRows,
        metadata.getDataType());
  }
  
  public List<Object> getValuesForRecord(int entityId) {
    // First extract the values from the byte buffer.
    // NOTE: The byte buffer is compacted meaning there is no deadspace.
    // So we must simply traverse the records in order until we find our ID,
    
    int counter = 0;
    int rowNum = 0;
    for (EntityRecord er : entityRecords) {
      if (er.getEntityId() == entityId) {
        int valueLength = er.getValueLength();
        int numRows = getDataType().determineNumValues(er.getValueLength());
        byte[] buffer = columnValues.array();
        byte[] output = new byte[valueLength];
        System.arraycopy(buffer, counter, output, 0, valueLength);
        // Convert the buffer array into the types we expect
        ByteBuffer bb = ByteBuffer.wrap(buffer);
        List<Object> values = getDataType().traverseByteBufferToList(bb, valueLength);
        
        // Now check the row number to see which one should be null.
        for (int i = 0; i < numRows; i++) {
          if (this.rowsIsNull.contains(rowNum + (i+1))) {
            values.set(i, null);
          }
        }
        return values;
      } else {
        counter += er.getValueLength();
        rowNum += getDataType().determineNumValues(er.getValueLength());        
      }
    }
    return null;
  }

  public void load(DataInputStream inputStream) throws IOException {    
    ColumnFileReader cfr = new ColumnFileReader();
    cfr.read(inputStream, (section, metadata, is, compressed, uncompressed) -> {
      try {
        if (section == ColumnFileSection.ENTITY_DICTIONARY) {
          return readEntityDictionary(is, compressed, uncompressed, metadata);
        } else if (section == ColumnFileSection.VALUE_DICTIONARY) {
          return readValueDictionary(is, compressed, uncompressed, metadata);
        } else if (section == ColumnFileSection.ENTITY_INDEX) {
          return readEntityIndex(is, compressed, uncompressed);
        } else if (section == ColumnFileSection.ROWGROUP) {
          return readRowGroup(is, compressed, uncompressed, metadata);
        } else
          return 0;
      } catch (IOException ioe) {
        LOGGER.error("Detected an error in reading section {}", section, ioe);
        throw new RuntimeException(ioe);
      }
    });
    metadata = cfr.getColumnMetadata();
  }

  
  private int loadToByteBuffer(List<EntityRecord> indexRecords, InputStream inputStream, ColumnMetadata metadata) throws IOException {
    columnValues = ByteBuffer.allocate(metadata.getDataType().determineByteLength(metadata.getNumRows()));
    entityNumRows = new int[metadata.getNumEntities()];
    entityDecodedLength = new int[metadata.getNumEntities()];
    DataType dataType = metadata.getDataType();
    int bytesRead = 0;
    int entityCounter = 0;

    byte[] nullBuffer = new byte[4096];
    int columnValuesArrayOffset = 0;
    int rowCounter = 0;
    byte[] columnValuesArray = columnValues.array();
    for (EntityRecord eir : indexRecords) {
      entityDecodedLength[entityCounter] = eir.getDecodedLength();
      int rowGroupOffset = eir.getRowGroupOffset();
      if (bytesRead < rowGroupOffset) {
        // Read up to the offset to skip.
        int skipBytes = rowGroupOffset - bytesRead;
        bytesRead += IOTools.skipFully(inputStream, skipBytes);
      }

      // Read into column values the encoded value portion 
      int valueLength = eir.getValueLength();
      bytesRead += IOTools.readFully(inputStream, columnValuesArray, columnValuesArrayOffset, valueLength);
      // Find out how many this equates to and record
      int numRows = dataType.determineNumValues(valueLength);
      entityNumRows[entityCounter] = dataType.determineNumValues(valueLength);

      // Update offsets, counters, etc.
      columnValuesArrayOffset += valueLength;

      // Read the next int to see if there is a nullbitmap
      int nullBitMapLength = eir.getNullLength();
      if (nullBitMapLength > 0) {
        if (nullBuffer.length < nullBitMapLength) {
          nullBuffer = new byte[nullBitMapLength * 2];
        }

        try {
          bytesRead += IOTools.readFully(inputStream, nullBuffer, 0, nullBitMapLength);
          RoaringBitmap roar = new RoaringBitmap();
          roar.deserialize(ByteBuffer.wrap(nullBuffer));
          for (int relativeRowPosition : roar.toArray()) {
            rowsIsNull.add(rowCounter + relativeRowPosition);
          }
        } catch (Exception e) {
          throw new RuntimeException("Unable to read column " + metadata.getColumnId(), e);
        }
      }
      rowCounter += numRows;
      entityCounter++;
    }
    return bytesRead;
  }
  
  @Override
  protected int readValueDictionary(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata) throws IOException {
    int read = super.readValueDictionary(inputStream, compressed, uncompressed, metadata);
    if (strValueDictionary == null)
      rowsIsNull = new IntArrayList(metadata.getNumRows());
    return read;
  }
  
  @Override
  protected int readEntityDictionary(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata)
    throws IOException {
    if (compressed > 0) {
      return (int) IOTools.skipFully(inputStream, compressed);
    } else if (uncompressed > 0) {
      return (int) IOTools.skipFully(inputStream, uncompressed);
    }
    return 0;
  }
  
  @Override
  protected int readRowGroup(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata) throws IOException {
    // Next row group
    if (compressed > 0) {
      ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
      int uncompressedRead = loadToByteBuffer(entityRecords, zstdInputStream, metadata);
      if (uncompressed != uncompressedRead) {
        LOGGER.warn("The expected number of bytes to be read for {} doesn't match {} read vs. {} expected, this can be an issue", this.columnId(), uncompressedRead, uncompressed);
      }
      return compressed;
    } else {
      return loadToByteBuffer(entityRecords, inputStream, metadata);
    }
  }
}
