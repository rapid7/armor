package com.rapid7.armor.read;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.roaringbitmap.RoaringBitmap;
import static com.rapid7.armor.Constants.MAGIC_HEADER;
import static com.rapid7.armor.Constants.RECORD_SIZE_BYTES;
import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.columns.Column;

/**
 * A slower version of an armor shard but comes with an easy to use api for exploring the shard data.
 * Use this version if speed is not the main concern. The shard is embedded with TableSaw which are good
 * for visualization. For production use with presto use FastArmorShard.
 */
public class SlowArmorShard {
  private ColumnMetadata metadata;
  private final Map<Object, List<Integer>> index = new HashMap<>();
  private DictionaryReader strValueDictionaryReader;
  private DictionaryReader entityDictionaryReader;
  private Column<?> column;
  private final IntColumn entityIndexColumn = IntColumn.create("id");

  public SlowArmorShard() {
  }

  public SlowArmorShard(InputStream inputStream) throws IOException {
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

  public int countRows() {
    return column.size();
  }

  public String columnName() {
    return metadata.getColumnName();
  }

  public IntColumn getEntitesInt() {
    return entityIndexColumn;
  }

  public Column<?> getColumn(Object entity) {
    switch (metadata.getDataType()) {
      case STRING:
        return getStrings(entity);
      case BOOLEAN:
        // return getBooleans(entity);
      case INTEGER:
        return getIntegers(entity);
      case DATETIME:
      case LONG:
        // return getLongs(entity);
      case DOUBLE:
        //return getDoubles(entity);
      default:
        break;
    }
    return null;
  }

  public Column<?> getColumn() {
    switch (metadata.getDataType()) {
      case STRING:
        return getStrings();
      case BOOLEAN:
        return getBooleans();
      case INTEGER:
        return getIntegers();
      case DATETIME:
      case LONG:
        return getLongs();
      case DOUBLE:
        return getDoubles();
      default:
        break;
    }
    return null;
  }

  public LongColumn getDates() {
    validateValueCall(DataType.LONG);
    return (LongColumn) column;
  }

  public BooleanColumn getBooleans() {
    validateValueCall(DataType.BOOLEAN);
    return (BooleanColumn) column;
  }

  public FloatColumn getFloats() {
    validateValueCall(DataType.FLOAT);
    return (FloatColumn) column;
  }

  public StringColumn getStrings() {
    validateValueCall(DataType.STRING);
    return (StringColumn) column;
  }

  public StringColumn getStrings(Object entityid) {
    validateValueCall(DataType.STRING);
    ArrayList<Integer> results = new ArrayList<>();
    List<Integer> rowNumbers = index.get(resolveEntity(entityid));
    if (rowNumbers == null)
      return StringColumn.create(metadata.getColumnName());

    for (Integer rowNum : rowNumbers) {
      results.add((Integer) column.get(rowNum));
    }
    return StringColumn.create(metadata.getColumnName(), results.toArray(new String[results.size()]));
  }


  public IntColumn getIntegers(Object entityid) {
    validateValueCall(DataType.INTEGER);
    ArrayList<Integer> results = new ArrayList<>();
    List<Integer> rowNumbers = index.get(resolveEntity(entityid));
    if (rowNumbers == null)
      return IntColumn.create(metadata.getColumnName());

    for (Integer rowNum : rowNumbers) {
      results.add((Integer) column.get(rowNum));
    }
    return IntColumn.create(metadata.getColumnName(), results.toArray(new Integer[results.size()]));
  }

  public IntColumn getIntegers() {
    validateValueCall(DataType.INTEGER);
    return (IntColumn) column;
  }

  public DoubleColumn getDoubles() {
    validateValueCall(DataType.DOUBLE);
    return (DoubleColumn) column;
  }

  public LongColumn getLongs() {
    validateValueCall(DataType.LONG);
    return (LongColumn) column;
  }

  private void readForMagicHeader(DataInputStream inputStream) throws IOException {
    short header = inputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("T" + header + " is not a match");
  }

  private Column<?> createColumn() {
    switch (metadata.getDataType()) {
      case STRING:
        return StringColumn.create(metadata.getColumnName());
      case INTEGER:
        return IntColumn.create(metadata.getColumnName());
      case DATETIME:
      case LONG:
        return LongColumn.create(metadata.getColumnName());
      default:
        break;
    }
    throw new IllegalArgumentException();
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

    // Read entity dictionary now
    int entityDictionaryCompressed = inputStream.readInt();
    int entityDictionaryOriginal = inputStream.readInt();
    if (entityDictionaryCompressed > 0) {
      byte[] compressedIndex = new byte[entityDictionaryCompressed];
      inputStream.read(compressedIndex);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, entityDictionaryOriginal);
      entityDictionaryReader = new DictionaryReader(new String(decomporessedIndex), metadata.getNumEntities(), true);
    } else if (entityDictionaryOriginal > 0) {
      byte[] decompressed = new byte[entityDictionaryCompressed];
      inputStream.read(decompressed);
      entityDictionaryReader = new DictionaryReader(new String(decompressed), metadata.getNumEntities(), true);
    }

    // Read value dictionary now
    int dictionaryCompressed = inputStream.readInt();
    int dictionaryOriginal = inputStream.readInt();
    if (dictionaryCompressed > 0) {
      byte[] compressedIndex = new byte[dictionaryCompressed];
      inputStream.read(compressedIndex);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, dictionaryOriginal);
      strValueDictionaryReader = new DictionaryReader(new String(decomporessedIndex), metadata.getCardinality(), false);
    } else if (dictionaryOriginal > 0) {
      byte[] decompressed = new byte[dictionaryOriginal];
      inputStream.read(decompressed);
      strValueDictionaryReader = new DictionaryReader(new String(decompressed), metadata.getCardinality(), false);
    }

    // Read entity
    int entityIndexCompressed = inputStream.readInt();
    int entityIndexOriginal = inputStream.readInt();
    List<EntityRecord> entityRecords;
    if (entityIndexCompressed > 0) {
      byte[] compressedIndex = new byte[entityIndexCompressed];
      inputStream.read(compressedIndex);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, entityIndexOriginal);
      try (DataInputStream uncompressedInputStream = new DataInputStream(new ByteArrayInputStream(decomporessedIndex))) {
        entityRecords = readAllIndexRecords(uncompressedInputStream, entityIndexOriginal);
      }
    } else {
      entityRecords = readAllIndexRecords(inputStream, entityIndexOriginal);
    }
    entityRecords = EntityRecord.sortActiveRecordsByOffset(entityRecords);
    // Next row group
    int rgCompressed = inputStream.readInt();
    int rgOriginal = inputStream.readInt();
    int readBytes = 0;
    if (rgCompressed > 0) {
      ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
      readBytes = readToTable(entityRecords, zstdInputStream);
    } else {
      readBytes = readToTable(entityRecords, inputStream);
    }
  }

  private int readToTable(List<EntityRecord> indexRecords, InputStream inputStream) throws IOException {
    int readBytes = 0;
    final AtomicInteger rowCounter = new AtomicInteger(1);
    DataType dt = metadata.getDataType();

    column = createColumn();

    for (EntityRecord eir : indexRecords) {
      int entity = eir.getEntityId();
      final List<Integer> rowNumbers;
      if (!index.containsKey(entity)) {
        rowNumbers = new ArrayList<>();
        index.put(entity, rowNumbers);
      } else {
        rowNumbers = index.get(entity);
      }

      int previousRowSize = column.size();
      int rowGroupOffset = eir.getRowGroupOffset();
      if (readBytes < rowGroupOffset) {
        // Read up to the offset to skip.
        inputStream.skip(rowGroupOffset - readBytes);
        readBytes += (rowGroupOffset - readBytes);
      }
      byte[] payload = new byte[eir.getValueLength()];
      inputStream.read(payload);
      DataInputStream dis = new DataInputStream(new ByteArrayInputStream(payload));
      dt.traverseDataInputStream(dis, eir.getValueLength(), r -> {
        entityIndexColumn.append(entity);
        rowNumbers.add(rowCounter.get() - 1);
        // Strings are surrogate ids, so get the string value for the surrogate.
        if (dt == DataType.STRING)
          column.appendObj(strValueDictionaryReader.getValueAsString((int) r));
        else
          column.appendObj(r);
        rowCounter.incrementAndGet();
      });
      readBytes += payload.length;

      // Read the next int to see if there is a nullbitmap
      int nullBitMapLength = eir.getNullLength();
      if (nullBitMapLength > 0) {
        byte[] buffer = new byte[nullBitMapLength];
        try {
          readBytes += inputStream.read(buffer);

          RoaringBitmap roar = new RoaringBitmap();
          roar.deserialize(ByteBuffer.wrap(buffer));

          for (int position : roar.toArray()) {
            int rowAdjusted = position + (previousRowSize - 1);
            column.set(rowAdjusted, null);
          }
        } catch (Exception e) {
          throw new RuntimeException("Unable to read column " + metadata.getColumnName(), e);
        }
      }
    }
    return readBytes;
  }

  private List<EntityRecord> readAllIndexRecords(DataInputStream inputStream, int originalLength) throws IOException {
    List<EntityRecord> records = new ArrayList<>();
    for (int i = 0; i < originalLength; i += RECORD_SIZE_BYTES) {
      EntityRecord eir = readIndexRecords(inputStream);
      records.add(eir);
    }
    return records;
  }

  private Integer resolveEntity(Object entityid) {
    if (entityid instanceof String) {
      return entityDictionaryReader.getSurrogate((String) entityid);
    }
    return (Integer) entityid;
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

  private void validateValueCall(DataType dataType) {
    if (metadata.getDataType() != dataType)
      throw new IllegalArgumentException("The data type " + dataType + " doesn't match the defined column data type of " + metadata.getDataType());
  }
}
