package com.rapid7.armor.read.slow;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdInputStream;
import com.rapid7.armor.ArmorSection;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.BaseArmorShardColumn;
import com.rapid7.armor.schema.DataType;

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
public class SlowArmorShardColumn extends BaseArmorShardColumn {
  private static final Logger LOGGER = LoggerFactory.getLogger(SlowArmorShardColumn.class);
  private final Map<Object, List<Integer>> entityToRowNumbers = new HashMap<>();
  private Column<?> column;
  private final IntColumn entityIndexColumn = IntColumn.create("id");

  public SlowArmorShardColumn() {}

  public SlowArmorShardColumn(InputStream inputStream) throws IOException {
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

  public Column<?> getColumnByEntityId(Object entityId) {
    switch (metadata.getDataType()) {
      case STRING:
        return getStringsByEntity(entityId);
      case BOOLEAN:
        // return getBooleans(entity);
      case INTEGER:
        return getIntegersByEntity(entityId);
      case DATETIME:
      case LONG:
        return getLongsByEntity(entityId);
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

  public StringColumn getStringsByEntity(Object entityid) {
    validateValueCall(DataType.STRING);
    ArrayList<String> results = new ArrayList<>();
    Integer surrogate = resolveEntity(entityid);
    List<Integer> rowNumbers = entityToRowNumbers.get(surrogate);
    if (rowNumbers == null)
      return StringColumn.create(metadata.getColumnName());

    for (Integer rowNum : rowNumbers) {
      results.add((String) column.get(rowNum));
    }
    return StringColumn.create(metadata.getColumnName(), results.toArray(new String[results.size()]));
  }
  
  public LongColumn getLongsByEntity(Object entityid) {
    validateValueCall(DataType.LONG);
    ArrayList<Long> results = new ArrayList<>();
    List<Integer> rowNumbers = entityToRowNumbers.get(resolveEntity(entityid));
    if (rowNumbers == null)
      return LongColumn.create(metadata.getColumnName());

    for (Integer rowNum : rowNumbers) {
      results.add((Long) column.get(rowNum));
    }
    long[] finalResults = new long[results.size()];
    for (int i = 0; i < results.size(); i++) {
      if (results.get(i) == null)
        finalResults[i] = Long.MIN_VALUE;
      else
        finalResults[i] = results.get(i);   
    }
    return LongColumn.create(metadata.getColumnName(), finalResults);
  }

  public IntColumn getIntegersByEntity(Object entityid) {
    validateValueCall(DataType.INTEGER);
    ArrayList<Integer> results = new ArrayList<>();
    List<Integer> rowNumbers = entityToRowNumbers.get(resolveEntity(entityid));
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

  private Column<?> createColumn(DataType dataType, String columnName) {
    switch (dataType) {
      case STRING:
        return StringColumn.create(columnName);
      case INTEGER:
        return IntColumn.create(columnName);
      case DATETIME:
      case LONG:
        return LongColumn.create(columnName);
      default:
        break;
    }
    throw new IllegalArgumentException();
  }

  public void load(DataInputStream inputStream) throws IOException {
    ColumnFileReader cfr = new ColumnFileReader();
    cfr.read(inputStream, (section, metadata, is, compressed, uncompressed) -> {
      try {
        if (section == ArmorSection.ENTITY_DICTIONARY) {
          return readEntityDictionary(is, compressed, uncompressed, metadata);
        } else if (section == ArmorSection.VALUE_DICTIONARY) {
          return readValueDictionary(is, compressed, uncompressed, metadata);
        } else if (section == ArmorSection.ENTITY_INDEX) {
          return readEntityIndex(is, compressed, uncompressed);
        } else if (section == ArmorSection.ROWGROUP) {
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

  private int readToTable(List<EntityRecord> indexRecords, InputStream inputStream, ColumnMetadata metadata) throws IOException {
    int readBytes = 0;
    final AtomicInteger rowCounter = new AtomicInteger(1);
    DataType dt = metadata.getDataType();

    column = createColumn(dt, metadata.getColumnName());

    for (EntityRecord eir : indexRecords) {
      int entity = eir.getEntityId();
      final List<Integer> rowNumbers;
      if (!entityToRowNumbers.containsKey(entity)) {
        rowNumbers = new ArrayList<>();
        entityToRowNumbers.put(entity, rowNumbers);
      } else {
        rowNumbers = entityToRowNumbers.get(entity);
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
          column.appendObj(strValueDictionary.getValueAsString((int) r));
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

  private Integer resolveEntity(Object entityid) {
    if (entityid instanceof String) {
      return entityDictionaryReader.getSurrogate((String) entityid);
    }
    return (Integer) entityid;
  }

  private void validateValueCall(DataType dataType) {
    if (metadata.getDataType() != dataType)
      throw new IllegalArgumentException("The data type " + dataType + " doesn't match the defined column data type of " + metadata.getDataType());
  }

  @Override
  protected int readRowGroup(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata) throws IOException {
    if (compressed > 0) {
      ZstdInputStream zstdInputStream = new ZstdInputStream(inputStream);
      return readToTable(entityRecords, zstdInputStream, metadata);
    } else {
      return readToTable(entityRecords, inputStream, metadata);
    }
  }
}
