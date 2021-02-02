package com.rapid7.armor.read;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.DataType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.MAGIC_HEADER;
import static com.rapid7.armor.Constants.RECORD_SIZE_BYTES;
import it.unimi.dsi.fastutil.ints.IntArrayList;

/**
 * An armor shard version that tries to provide the column data as fast as possible.
 * <p>
 * 1) Avoid returning objects.
 * 2) Avoid unnecssary alloc/dealloc
 * 3) Avoid unnessary looping
 */
public class FastArmorShard {
  private static final Logger LOGGER = LoggerFactory.getLogger(FastArmorShard.class);
  private ColumnMetadata metadata;
  private DictionaryReader strValueDictionary;
  private List<EntityRecord> entityRecords;
  private ByteBuffer columnValues;
  private int[] entityNumRows;
  private int[] entityDecodedLength;
  private IntArrayList rowsIsNull; // Row number that is null (NOTE: NOT zero-indexed)

  public FastArmorShard(InputStream inputStream) throws IOException {
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

  public String columnName() {
    return metadata.getColumnName();
  }

  public FastArmorColumnReader getFastReader() {
    return new FastArmorColumnReader(
        columnValues,
        rowsIsNull,
        strValueDictionary,
        metadata.getNumRows(),
        metadata.getNumEntities(),
        entityDecodedLength,
        entityNumRows);
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

  private void readForMagicHeader(DataInputStream inputStream) throws IOException {
    short header = inputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("T" + header + " is not a match");
  }

  public void load(DataInputStream inputStream) throws IOException {
    // Header first
    readForMagicHeader(inputStream);

    // Next metadata
    inputStream.readInt(); // Metdata never compressed
    int metadataLength = inputStream.readInt(); // Metdata uncomprossed
    byte[] metadataBytes = new byte[metadataLength];
    inputStream.readFully(metadataBytes);
    metadata = new ObjectMapper().readValue(metadataBytes, ColumnMetadata.class);


    // 2) Initialize what we need.
    columnValues = ByteBuffer.allocate(metadata.getDataType().determineByteLength(metadata.getNumRows()));
    entityNumRows = new int[metadata.getNumEntities()];
    entityDecodedLength = new int[metadata.getNumEntities()];

    // Skip here, the entity id column should hold this info.
    int entityDictionaryCompressed = inputStream.readInt();
    int entityDictionaryOriginal = inputStream.readInt();
    if (entityDictionaryCompressed > 0) {
      IOTools.skipFully(inputStream, entityDictionaryCompressed);
    } else if (entityDictionaryOriginal > 0) {
      IOTools.skipFully(inputStream, entityDictionaryOriginal);
    }

    // Read string value dictionary (if required)
    int dictionaryCompressed = inputStream.readInt();
    int dictionaryOriginal = inputStream.readInt();
    if (dictionaryCompressed > 0) {
      byte[] compressedIndex = new byte[dictionaryCompressed];
      IOTools.readFully(inputStream, compressedIndex, 0, dictionaryCompressed);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, dictionaryOriginal);
      strValueDictionary = new DictionaryReader(new String(decomporessedIndex), metadata.getCardinality(), false);
    } else if (dictionaryOriginal > 0) {
      byte[] decompressed = new byte[dictionaryOriginal];
      IOTools.readFully(inputStream, decompressed, 0, dictionaryOriginal);
      strValueDictionary = new DictionaryReader(new String(decompressed), metadata.getCardinality(), false);
    }

    if (strValueDictionary == null)
      rowsIsNull = new IntArrayList(metadata.getNumRows());

    // Read entity
    int entityIndexCompressed = inputStream.readInt();
    int entityIndexOriginal = inputStream.readInt();
    if (entityIndexCompressed > 0) {
      byte[] compressedIndex = new byte[entityIndexCompressed];
      IOTools.readFully(inputStream, compressedIndex, 0, entityIndexCompressed);
      byte[] decompressedIndex = Zstd.decompress(compressedIndex, entityIndexOriginal);
      try (DataInputStream uncompressedInputStream = new DataInputStream(new ByteArrayInputStream(decompressedIndex))) {
        entityRecords = readAllIndexRecords(uncompressedInputStream, entityIndexOriginal);
      }
    } else {
      byte[] entityIndexBytes = new byte[entityIndexOriginal];
      IOTools.readFully(inputStream, entityIndexBytes, 0, entityIndexOriginal);
      try (DataInputStream uncompressedInputStream = new DataInputStream(new ByteArrayInputStream(entityIndexBytes))) {
        entityRecords = readAllIndexRecords(uncompressedInputStream, entityIndexOriginal);
      }
    }
    entityRecords = EntityRecord.sortActiveRecordsByOffset(entityRecords);

    // Next row group
    int rgCompressed = inputStream.readInt();
    int rgUncompressed = inputStream.readInt();
    if (rgCompressed > 0) {
      ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
      loadToByteBuffer(entityRecords, zstdInputStream);
    } else {
      loadToByteBuffer(entityRecords, inputStream);
    }
  }

  private int loadToByteBuffer(List<EntityRecord> indexRecords, InputStream inputStream) throws IOException {
    int bytesRead = 0;
    int entityCounter = 0;

    byte[] nullBuffer = new byte[4096];
    int columnValuesArrayOffset = 0;
    int rowCounter = 0;
    DataType dataType = metadata.getDataType();
    byte[] columnValuesArray = columnValues.array();
    for (EntityRecord eir : indexRecords) {
      entityDecodedLength[entityCounter] = eir.getDecodedLength();
      int rowGroupOffset = eir.getRowGroupOffset();
      if (bytesRead < rowGroupOffset) {
        // Read up to the offset to skip.
        int skipBytes = rowGroupOffset - bytesRead;
        IOTools.skipFully(inputStream, skipBytes);
        bytesRead += skipBytes;
      }

      // Read into column values the encoded value portion 
      int valueLength = eir.getValueLength();
      IOTools.readFully(inputStream, columnValuesArray, columnValuesArrayOffset, valueLength);
      // Find out how many this equates to and record
      int numRows = dataType.determineNumValues(valueLength);
      entityNumRows[entityCounter] = dataType.determineNumValues(valueLength);

      // Update offsets, counters, etc.
      columnValuesArrayOffset += valueLength;
      bytesRead += valueLength;

      // Read the next int to see if there is a nullbitmap
      int nullBitMapLength = eir.getNullLength();
      if (nullBitMapLength > 0) {
        if (nullBuffer.length < nullBitMapLength) {
          nullBuffer = new byte[nullBitMapLength * 2];
        }

        try {
          bytesRead += IOTools.readFully(inputStream, nullBuffer, 0, nullBitMapLength);
          //bytesRead += inputStream.read(nullBuffer, 0, nullBitMapLength);
          RoaringBitmap roar = new RoaringBitmap();
          roar.deserialize(ByteBuffer.wrap(nullBuffer));
          for (int relativeRowPosition : roar.toArray()) {
            rowsIsNull.add(rowCounter + relativeRowPosition);
          }
        } catch (Exception e) {
          throw new RuntimeException("Unable to read column " + metadata.getColumnName(), e);
        }
      }
      rowCounter += numRows;
      entityCounter++;
    }
    return bytesRead;
  }

  private List<EntityRecord> readAllIndexRecords(DataInputStream inputStream, int originalLength) throws IOException {
    List<EntityRecord> records = new ArrayList<>();
    for (int i = 0; i < originalLength; i += RECORD_SIZE_BYTES) {
      EntityRecord eir = readIndexRecords(inputStream);
      if (eir.getDeleted() == 0)
        records.add(eir);
    }
    return records;
  }

  private EntityRecord readIndexRecords(DataInputStream inputStream) throws IOException {
    int entityUuid = inputStream.readInt();
    int rowGroupOffset = inputStream.readInt();
    int length = inputStream.readInt();
    long version = inputStream.readLong();
    byte deleted = inputStream.readByte();
    int nullLength = inputStream.readInt();
    int decodedLength = inputStream.readInt();
    byte[] instanceId = new byte[Constants.INSTANCE_ID_BYTE_LENGTH];
    inputStream.read(instanceId, 0, Constants.INSTANCE_ID_BYTE_LENGTH);
    return new EntityRecord(entityUuid, rowGroupOffset, length, version, deleted, nullLength, decodedLength, instanceId);
  }
}
