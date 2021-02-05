package com.rapid7.armor.write;

import com.rapid7.armor.ArmorSection;
import com.rapid7.armor.Constants;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.io.AutoDeleteFileInputStream;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.write.component.DictionaryWriter;
import com.rapid7.armor.write.component.EntityRecordWriter;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.component.RowGroupWriter.RgOffsetWriteResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.MAGIC_HEADER;

public class ColumnWriter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnWriter.class);
  private static final ObjectMapper om = new ObjectMapper();
  private EntityRecordWriter entityRecordWriter;
  private RowGroupWriter rowGroupWriter;
  private ColumnMetadata metadata;
  private DictionaryWriter strValueDictionary;
  private DictionaryWriter entityDictionary;
  private final ColumnShardId columnShardId;

  public ColumnWriter(ColumnShardId columnShardId) throws IOException {
    metadata = new ColumnMetadata();
    DataType dataType = columnShardId.getColumnName().dataType();
    this.columnShardId = columnShardId;
    metadata.setDataType(columnShardId.getColumnName().dataType());
    metadata.setColumnName(columnShardId.getColumnName().getName());
    columnShardId.getColumnName().dataType();
    if (dataType == DataType.STRING)
      strValueDictionary = new DictionaryWriter(false);

    entityDictionary = new DictionaryWriter(true);
    String name = columnShardId.alternateString() + ".armor";
    rowGroupWriter = new RowGroupWriter(Files.createTempFile("rowgroupstore_" + name + "-", ".armor"), columnShardId, strValueDictionary);
    entityRecordWriter = new EntityRecordWriter(Files.createTempFile("entityindexstore_" + name + "-", ".armor"), columnShardId);
  }

  public ColumnWriter(DataInputStream dataInputStream, ColumnShardId columnShardId) {
    try {
      DataType dt = columnShardId.getColumnName().dataType();
      int avail = dataInputStream.available();
      this.columnShardId = columnShardId;
      if (avail > 0) {
        try {
          if (dt == DataType.STRING)
            strValueDictionary = new DictionaryWriter(false);
          entityDictionary = new DictionaryWriter(true);
          load(dataInputStream);
        } finally {
          dataInputStream.close();
        }
      } else {
        metadata = new ColumnMetadata();
        metadata.setDataType(dt);
        metadata.setColumnName(columnShardId.getColumnName().getName());
        metadata.setLastUpdate(new Date().toString());
        if (dt == DataType.STRING)
          strValueDictionary = new DictionaryWriter(false);
        entityDictionary = new DictionaryWriter(true);
        rowGroupWriter = new RowGroupWriter(Files.createTempFile("rowgroupstore_" + columnShardId.alternateString() + "-", ".armor"), columnShardId, strValueDictionary);
        entityRecordWriter = new EntityRecordWriter(Files.createTempFile("entityindexstore_" + columnShardId.alternateString() + "-", ".armor"), columnShardId);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public List<EntityRecord> allEntityRecords() {
    try {
      return entityRecordWriter.allRecords();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public RowGroupWriter getRowGroupWriter() {
    return rowGroupWriter;
  }
  
  public EntityRecordWriter getEntityRecordWriter() {
    return entityRecordWriter;
  }

  public Map<Integer, EntityRecord> getEntites() {
    return entityRecordWriter.getEntities();
  }

  public DictionaryWriter getEntityDictionary() {
    return entityDictionary;
  }

  @Override
  public void close() {
    try {
      rowGroupWriter.close();
    } catch (IOException ioe) {
      LOGGER.error("Unable to close rowGroupWriter on {}", columnShardId, ioe);
    }
    try {
      entityRecordWriter.close();
    } catch (IOException ioe) {
      LOGGER.error("Unable to close entityRecordWriter writer on {}", columnShardId, ioe);
    }
  }

  public ColumnMetadata getMetadata() {
    return metadata;
  }

  public ColumnShardId getColumnShardId() {
    return columnShardId;
  }

  public ColumnName getColumnName() {
    return columnShardId.getColumnName();
  }
  
  private void loadEntityDictionary(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    // Load entity dictionary
    if (compressed > 0) {
      byte[] compressedDict = new byte[compressed];
      IOTools.readFully(inputStream, compressedDict, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedDict, uncompressed);
      entityDictionary = new DictionaryWriter(decompressed, true);
      decompressed = null;
    } else if (uncompressed > 0) {
      byte[] uncompressedDict = new byte[uncompressed];
      IOTools.readFully(inputStream, uncompressedDict, 0, uncompressed);
      entityDictionary = new DictionaryWriter(uncompressedDict, true);
    }
  }
  
  private void loadValueDictionary(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    // Load str value dictionary
    if (compressed > 0) {
      byte[] compressedDict = new byte[compressed];
      IOTools.readFully(inputStream, compressedDict, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedDict, uncompressed);
      strValueDictionary = new DictionaryWriter(decompressed, false);
      decompressed = null;
    } else if (uncompressed > 0) {
      byte[] uncompressedDict = new byte[uncompressed];
      IOTools.readFully(inputStream, uncompressedDict, 0, uncompressed);
      strValueDictionary = new DictionaryWriter(uncompressedDict, false);
    }
  }
  
  private Path loadEntityIndex(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    String tempName = columnShardId.alternateString() + "-";
    Path entityIndexTemp = Files.createTempFile("entityindex_" + tempName, ".armor");
    if (compressed > 0) {
      byte[] compressedIndex = new byte[compressed];
      IOTools.readFully(inputStream, compressedIndex, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedIndex, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(decompressed)) {
        Files.copy(bais, entityIndexTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      decompressed = null;
      entityRecordWriter = new EntityRecordWriter(entityIndexTemp, columnShardId);
    } else {
      byte[] uncompressedIndex = new byte[uncompressed];
      IOTools.readFully(inputStream, uncompressedIndex, 0, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(uncompressedIndex)) {
        Files.copy(bais, entityIndexTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      entityRecordWriter = new EntityRecordWriter(entityIndexTemp, columnShardId);
    }
    return entityIndexTemp;
  }
  
  private Path loadRowGroup(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    Path rgGroupTemp = Files.createTempFile("rowgroupstore_" + columnShardId.alternateString() + "-", ".armor");
    if (compressed > 0) {
      byte[] compressedRg = new byte[compressed];
      IOTools.readFully(inputStream, compressedRg, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedRg, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(decompressed)) {
        Files.copy(bais, rgGroupTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      decompressed = null;
      rowGroupWriter = new RowGroupWriter(rgGroupTemp, columnShardId, strValueDictionary);
      rowGroupWriter.position(rowGroupWriter.getCurrentSize());
    } else {
      byte[] uncompressedRg = new byte[uncompressed];
      IOTools.readFully(inputStream, uncompressedRg, 0, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(uncompressedRg)) {
        Files.copy(bais, rgGroupTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      rowGroupWriter = new RowGroupWriter(rgGroupTemp, columnShardId, strValueDictionary);
      rowGroupWriter.position(rowGroupWriter.getCurrentSize());
    }
    return rgGroupTemp;
  }

  // Read from the input streams to setup the writer.
  private void load(DataInputStream inputStream) throws IOException {
    ColumnFileReader cfr = new ColumnFileReader();
    List<Path> tempPaths = new ArrayList<>();
    boolean success = false;
    try {
      cfr.read(inputStream, (section, is, compressed, uncompressed) -> {
        try {
          if (section == ArmorSection.ENTITY_DICTIONARY) {
            loadEntityDictionary(is, compressed, uncompressed);
          } else if (section == ArmorSection.VALUE_DICTIONARY) {
            loadValueDictionary(is, compressed, uncompressed);
          } else if (section == ArmorSection.ENTITY_INDEX) {
            tempPaths.add(loadEntityIndex(is, compressed, uncompressed));
          } else if (section == ArmorSection.ROWGROUP) {
            tempPaths.add(loadRowGroup(inputStream, compressed, uncompressed));
          }
        } catch (IOException ioe) {
          LOGGER.error("Detected an error in reading section {}", section, ioe);
          throw new RuntimeException(ioe);
        }
      });
      metadata = cfr.getColumnMetadata();
      metadata.setLastUpdate(new Date().toString());
      success = true;
    } finally {
      if (!success) {
        for (Path path : tempPaths) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  /**
   * Returns a list of sorted entity ids, this can be used to verify integrity of columns before sending over.
   */
  public List<EntityRecordSummary> getEntityRecordSummaries() {
    int byteLength = metadata.getDataType().getByteLength();
    List<EntityRecord> records = entityRecordWriter.getEntityRecords(entityDictionary);
    if (entityDictionary.isEmpty()) {
      return records.stream()
          .map(e -> new EntityRecordSummary(
              e.getEntityId(),
              e.getValueLength() / byteLength,
              e.getRowGroupOffset(),
              e.getVersion(),
              e.getInstanceId()))
          .filter(e -> e.getNumRows() > 0).collect(Collectors.toList());
    } else {
      return records.stream()
          .map(e -> new EntityRecordSummary(
              entityDictionary.getValue(e.getEntityId()),
              e.getValueLength() / byteLength, e.getRowGroupOffset(),
              e.getVersion(),
              e.getInstanceId()))
          .filter(e -> e.getNumRows() > 0).collect(Collectors.toList());
    }
  }
  
  public StreamProduct buildInputStream(boolean compress) throws IOException {
    int totalBytes = 0;
    ByteArrayOutputStream headerPortion = new ByteArrayOutputStream();
    writeForMagicHeader(headerPortion);

    // Prepare metadata for writing
    metadata.setLastUpdate(new Date().toString());
    if (compress)
      metadata.setCompressionAlgorithm("zstd"); // Currently we only support this.
    else
      metadata.setCompressionAlgorithm("none");
    List<EntityRecord> records = entityRecordWriter.getEntityRecords(entityDictionary);
    entityRecordWriter.runThroughRecords(metadata, records);
    // Run through the values to update metadata
    rowGroupWriter.runThoughValues(metadata, records);
    // Store metadata
    String metadataStr = om.writeValueAsString(metadata);
    byte[] metadataPayload = metadataStr.getBytes();
    writeLength(headerPortion, 0, metadataPayload.length);
    headerPortion.write(metadataPayload);

    // Send entity dictionary
    InputStream entityDictIs;
    ByteArrayInputStream entityDictionaryLengths;
    List<Path> tempPaths = new ArrayList<>();
    totalBytes += 8;
    boolean success = false;
    try {
      if (entityDictionary.isEmpty()) {
        entityDictionaryLengths = new ByteArrayInputStream(writeLength(0, 0));
        entityDictIs = new ByteArrayInputStream(new byte[0]);
      } else {
        if (compress) {
          Path entityDictTempPath = Files.createTempFile("entity-dict-temp_" + columnShardId.alternateString(), ".armor");
          tempPaths.add(entityDictTempPath);
          try (ZstdOutputStream zstdOutput = new ZstdOutputStream(new FileOutputStream(entityDictTempPath.toFile()), RecyclingBufferPool.INSTANCE);
               InputStream inputStream = entityDictionary.getInputStream()) {
            IOTools.copy(inputStream, zstdOutput);
          }
          int dictionaryLength = (int) Files.size(entityDictTempPath);
          entityDictionaryLengths = new ByteArrayInputStream(writeLength(dictionaryLength, (int) entityDictionary.getCurrentSize()));
          entityDictIs = new AutoDeleteFileInputStream(entityDictTempPath);
          totalBytes += dictionaryLength;
        } else {
          totalBytes += (int) entityDictionary.getCurrentSize();
          entityDictionaryLengths = new ByteArrayInputStream(writeLength(0, (int) entityDictionary.getCurrentSize()));
          entityDictIs = entityDictionary.getInputStream();
        }
      }

      // Send strValue dictionary;
      InputStream valueDictIs;
      ByteArrayInputStream valueDictLengths;
      totalBytes += 8;
      if (strValueDictionary != null) {
        if (compress) {
          Path valueDictTempPath = Files.createTempFile("value-dict-temp_" + columnShardId.alternateString() + "-", ".armor");
          tempPaths.add(valueDictTempPath);
          try (ZstdOutputStream zstdOutput = new ZstdOutputStream(new FileOutputStream(valueDictTempPath.toFile()), RecyclingBufferPool.INSTANCE);
               InputStream inputStream = strValueDictionary.getInputStream()) {
            IOTools.copy(inputStream, zstdOutput);
          }
          int dictionaryLength = (int) Files.size(valueDictTempPath);
          totalBytes += dictionaryLength;
          valueDictLengths = new ByteArrayInputStream(writeLength((int) Files.size(valueDictTempPath), (int) strValueDictionary.getCurrentSize()));
          valueDictIs = new AutoDeleteFileInputStream(valueDictTempPath);
        } else {
          totalBytes += (int) strValueDictionary.getCurrentSize();
          valueDictLengths = new ByteArrayInputStream(writeLength(0, (int) strValueDictionary.getCurrentSize()));
          valueDictIs = strValueDictionary.getInputStream();
        }
      } else {
        valueDictLengths = new ByteArrayInputStream(writeLength(0, 0));
        valueDictIs = new ByteArrayInputStream(new byte[0]);
      }

      // Send entity index
      InputStream entityIndexIs;
      ByteArrayInputStream entityIndexLengths;
      totalBytes += 8;
      if (compress) {
        String tempName = this.columnShardId.alternateString();
        Path eiTempPath = Files.createTempFile("entity-temp_" + tempName + "-", ".armor");
        tempPaths.add(eiTempPath);
        try (ZstdOutputStream zstdOutput = new ZstdOutputStream(new FileOutputStream(eiTempPath.toFile()), RecyclingBufferPool.INSTANCE);
             InputStream inputStream = entityRecordWriter.getInputStream()) {
          IOTools.copy(inputStream, zstdOutput);
        }
        int payloadSize = (int) Files.size(eiTempPath);
        totalBytes += payloadSize;
        entityIndexLengths = new ByteArrayInputStream(writeLength(payloadSize, (int) entityRecordWriter.getCurrentSize()));
        entityIndexIs = new AutoDeleteFileInputStream(eiTempPath);
      } else {
        totalBytes += (int) entityRecordWriter.getCurrentSize();
        entityIndexLengths = new ByteArrayInputStream(writeLength(0, (int) entityRecordWriter.getCurrentSize()));
        entityIndexIs = entityRecordWriter.getInputStream();
      }

      // Send row group
      InputStream rgIs;
      ByteArrayInputStream rgLengths;
      totalBytes += 8;
      if (compress) {
        String tempName = columnShardId.alternateString();
        Path rgTempPath = Files.createTempFile("rowgroup-temp_" + tempName + "-", ".armor");
        tempPaths.add(rgTempPath);
        try (ZstdOutputStream zstdOutput = new ZstdOutputStream(new FileOutputStream(rgTempPath.toFile()), RecyclingBufferPool.INSTANCE);
             InputStream rgInputStream = rowGroupWriter.getInputStream()) {
          IOTools.copy(rgInputStream, zstdOutput);
        }
        int payloadSize = (int) Files.size(rgTempPath);
        totalBytes += payloadSize;
        rgLengths = new ByteArrayInputStream(writeLength((int) Files.size(rgTempPath), (int) rowGroupWriter.getCurrentSize()));
        rgIs = new AutoDeleteFileInputStream(rgTempPath);
      } else {
        totalBytes += (int) rowGroupWriter.getCurrentSize();
        rgLengths = new ByteArrayInputStream(writeLength(0, (int) rowGroupWriter.getCurrentSize()));
        rgIs = rowGroupWriter.getInputStream();
      }

      byte[] header = headerPortion.toByteArray();
      totalBytes += header.length;
      StreamProduct product = new StreamProduct(totalBytes, new SequenceInputStream(Collections.enumeration(Arrays.asList(
          new ByteArrayInputStream(header),
          entityDictionaryLengths,
          entityDictIs,
          valueDictLengths,
          valueDictIs,
          entityIndexLengths,
          entityIndexIs,
          rgLengths,
          rgIs))));
      success = true;
      return product;
    } finally {
      if (!success) {
        for (Path path : tempPaths) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  private byte[] writeLength(int compressed, int uncompressed) throws IOException {
    byte[] lengths = new byte[8];
    System.arraycopy(IOTools.toByteArray(compressed), 0, lengths, 0, 4);
    System.arraycopy(IOTools.toByteArray(uncompressed), 0, lengths, 4, 4);
    return lengths;
  }

  private void writeLength(OutputStream outputStream, int compressed, int uncompressed) throws IOException {
    outputStream.write(IOTools.toByteArray(compressed));
    outputStream.write(IOTools.toByteArray(uncompressed));
  }

  public void delete(String transaction, Object entity) {
    int entityId;
    if (entity instanceof String) {
      entityId = entityDictionary.getSurrogate((String) entity);
      entityDictionary.removeSurrogate((String) entity);
    } else if (entity instanceof Long)
      entityId = ((Long) entity).intValue();
    else if (entity instanceof Integer)
      entityId = ((Integer) entity);
    else
      throw new IllegalArgumentException("The entity type of " + entity.getClass().toString() + " is not supported for identity");
    delete(transaction, entityId);
  }

  public void delete(String transaction, int entity) {
    try {
      entityRecordWriter.delete(entity);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private int getEntityId(Object entity) {
    Integer entityInt;
    if (entity instanceof String) {
      entityInt = entityDictionary.getSurrogate((String) entity);
    } else if (entity instanceof Long) {
      entityInt = ((Long) entity).intValue();
    } else if (entity instanceof Integer) {
      entityInt = ((Integer) entity);
    } else
      throw new IllegalArgumentException("Entity uuids must be string, long or int");
    return entityInt;
  }

  public void write(String transaction, List<WriteRequest> writeRequests) throws IOException {
    // First filter out already old stuff that doesn't need to be written.
    HashMap<Object, WriteRequest> groupByMax = new HashMap<>();
    for (WriteRequest wr : writeRequests) {
      WriteRequest maxVersionWriteRequest = groupByMax.get(wr.getEntityId());
      if (maxVersionWriteRequest == null) {
        EntityRecord er = entityRecordWriter.getEntityRecord(getEntityId(wr.getEntityId()));
        if (er == null || er.getVersion() <= wr.getVersion())
          groupByMax.put(wr.getEntityId(), wr);
      } else {
        if (maxVersionWriteRequest.getVersion() <= wr.getVersion())
          groupByMax.put(wr.getEntityId(), wr);
      }
    }

    // Next bulk write to offset group, getting the relative rowGroupOffset for each in the array.
    List<Object[]> payloads = groupByMax.values().stream().map(WriteRequest::values).collect(Collectors.toList());
    List<WriteRequest> payloadColumns = new ArrayList<>(groupByMax.values());

    List<RgOffsetWriteResult> positions = rowGroupWriter.appendEntityValues(payloads);
    for (int i = 0; i < payloadColumns.size(); i++) {
      WriteRequest writeRequest = payloadColumns.get(i);
      RgOffsetWriteResult offsetResults = positions.get(i);

      EntityRecord er = new EntityRecord(
          getEntityId(writeRequest.getEntityId()),
          (int) offsetResults.rowGroupOffset,
          (int) offsetResults.valueLength,
          writeRequest.getVersion(),
          (byte) 0,
          (int) offsetResults.nullLength,
          (int) offsetResults.decodedLength,
          writeRequest.getInstanceId() == null ? null : writeRequest.getInstanceId().getBytes());
      entityRecordWriter.putEntity(er);
    }
  }

  private void writeForMagicHeader(OutputStream outputStream) throws IOException {
    outputStream.write(IOTools.toByteArray(MAGIC_HEADER));
  }

  private void readForMagicHeader(DataInputStream dataInputStream) throws IOException {
    short header = dataInputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("The magic header doesn't exist");
  }

  public void defrag() throws IOException {
    defrag(getEntityRecordSummaries());
  }

  // Defrag requires a list of entities to use, it can either be preexisting or non-existing.
  public void defrag(List<EntityRecordSummary> recordSummaries) throws IOException {
    List<EntityRecord> records = new ArrayList<>();
    for (EntityRecordSummary entityCheck : recordSummaries) {
      final Integer entityId;
      if (entityCheck.getId() instanceof String) {
        entityId = entityDictionary.getSurrogate((String) entityCheck.getId());
      } else {
        entityId = (Integer) entityCheck.getId();
      }

      EntityRecord eRecord = entityRecordWriter.getEntityRecord(entityId);
      if (eRecord == null) {
        // This doesn't exist, so fill it in
        int valueLength = metadata.getDataType().determineByteLength(entityCheck.getNumRows());
        EntityRecord er = new EntityRecord(
            entityId,
            Constants.NULL_FILLER_INDICATOR, // Trigger null fill in row group
            valueLength,
            entityCheck.getVersion(),
            (byte) 0,
            0,
            valueLength,
            entityCheck.getInstanceId() == null ? null : entityCheck.getInstanceId().getBytes());
        records.add(er);
      } else {
        records.add(eRecord);
      }
    }

    // With a list of sorted records, lets start the process of defrag
    List<EntityRecord> adjustedRecords = rowGroupWriter.defrag(records);
    entityRecordWriter.defrag(adjustedRecords);
    metadata.setLastDefrag(new Date().toString());
  }
}
