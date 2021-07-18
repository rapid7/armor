package com.rapid7.armor.write.writers;

import com.rapid7.armor.Constants;
import com.rapid7.armor.columnfile.ColumnFileSection;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.EntityRecordSummary;
import com.rapid7.armor.io.AutoDeleteFileInputStream;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.write.StreamProduct;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.component.Component;
import com.rapid7.armor.write.component.DictionaryWriter;
import com.rapid7.armor.write.component.EntityIndexVariableWidthException;
import com.rapid7.armor.write.component.EntityIndexWriter;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.component.RowGroupWriter.RgOffsetWriteResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdOutputStream;

import static com.rapid7.armor.Constants.DEFAULT_VERSION;
import static com.rapid7.armor.Constants.MAGIC_HEADER;

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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ColumnFileWriter implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ColumnFileWriter.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private EntityIndexWriter entityIndexWriter;
  private RowGroupWriter rowGroupWriter;
  private ColumnMetadata metadata;
  private DictionaryWriter valueDictionary;
  private DictionaryWriter entityDictionary;
  private final ColumnShardId columnShardId;
  private final String ROWGROUP_STORE_SUFFIX = "_rowgroup-";
  private final String ENTITYINDEX_STORE_SUFFIX = "_entityindex-";
  private boolean skipMetaData = false;
  
  public void setSkipMetaData(boolean skipMetaData) {
	this.skipMetaData = skipMetaData;
  }

  public ColumnFileWriter(ColumnShardId columnShardId) throws IOException {
    metadata = new ColumnMetadata();
    DataType dataType = columnShardId.getColumnId().dataType();
    this.columnShardId = columnShardId;
    metadata.setColumnType(columnShardId.getColumnId().dataType());
    metadata.setColumnName(columnShardId.getColumnId().getName());
    columnShardId.getColumnId().dataType();
    if (dataType == DataType.STRING)
      valueDictionary = new DictionaryWriter(false);

    entityDictionary = new DictionaryWriter(true);
    rowGroupWriter = new RowGroupWriter(TempFileUtil.createTempFile(columnShardId.alternateString() + ROWGROUP_STORE_SUFFIX, ".armor"), columnShardId, valueDictionary);
    entityIndexWriter = new EntityIndexWriter(TempFileUtil.createTempFile(columnShardId.alternateString() + ENTITYINDEX_STORE_SUFFIX, ".armor"), columnShardId);
  }
  
  public void checkForConsistency() {
    if (entityDictionary.getUsed() && metadata.getColumnType() == DataType.STRING && entityDictionary.realSize() != entityIndexWriter.getActiveEntities().size()) {
        throw new RuntimeException("TEST TEST Detected an error");
    }
  }

  public ColumnFileWriter(DataInputStream dataInputStream, ColumnShardId columnShardId) {
    try {
      DataType dt = columnShardId.getColumnId().dataType();
      int avail = dataInputStream.available();
      this.columnShardId = columnShardId;
      if (avail > 0) {
        try {
          if (dt == DataType.STRING)
            valueDictionary = new DictionaryWriter(false);
          entityDictionary = new DictionaryWriter(true);
          load(dataInputStream);
        } finally {
          dataInputStream.close();
        }
      } else {
        metadata = new ColumnMetadata();
        metadata.setColumnType(dt);
        metadata.setColumnName(columnShardId.getColumnId().getName());
        metadata.setLastUpdate(new Date().toString());
        if (dt == DataType.STRING)
          valueDictionary = new DictionaryWriter(false);
        entityDictionary = new DictionaryWriter(true);
        rowGroupWriter = new RowGroupWriter(TempFileUtil.createTempFile(columnShardId.alternateString() + ROWGROUP_STORE_SUFFIX, ".armor"), columnShardId, valueDictionary);
        entityIndexWriter = new EntityIndexWriter(TempFileUtil.createTempFile(columnShardId.alternateString() + ENTITYINDEX_STORE_SUFFIX, ".armor"), columnShardId);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public List<EntityRecord> allEntityRecords() {
    try {
      return entityIndexWriter.allRecords();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
  
  public RowGroupWriter getRowGroupWriter() {
    return rowGroupWriter;
  }
  
  public EntityIndexWriter getEntityRecordWriter() {
    return entityIndexWriter;
  }

  public Map<Integer, EntityRecord> getEntities() {
    return entityIndexWriter.getEntities();
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
      entityIndexWriter.close();
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

  public ColumnId getColumnId() {
    return columnShardId.getColumnId();
  }
  
  private int loadEntityDictionary(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    // Load entity dictionary
    int read = 0;
    if (compressed > 0) {
      byte[] compressedDict = new byte[compressed];
      read = IOTools.readFully(inputStream, compressedDict, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedDict, uncompressed);
      entityDictionary = new DictionaryWriter(decompressed, true);
    } else if (uncompressed > 0) {
      byte[] uncompressedDict = new byte[uncompressed];
      read = IOTools.readFully(inputStream, uncompressedDict, 0, uncompressed);
      entityDictionary = new DictionaryWriter(uncompressedDict, true);
    }
    return read;
  }
  
  private int loadValueDictionary(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    // Load str value dictionary
    int read = 0;
    if (compressed > 0) {
      byte[] compressedDict = new byte[compressed];
      read = IOTools.readFully(inputStream, compressedDict, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedDict, uncompressed);
      valueDictionary = new DictionaryWriter(decompressed, false);
    } else if (uncompressed > 0) {
      byte[] uncompressedDict = new byte[uncompressed];
      read = IOTools.readFully(inputStream, uncompressedDict, 0, uncompressed);
      valueDictionary = new DictionaryWriter(uncompressedDict, false);
    }
    return read;
  }
  
  private int loadEntityIndex(DataInputStream inputStream, int compressed, int uncompressed, List<Path> temps) throws IOException {
    Path entityIndexTemp = TempFileUtil.createTempFile(columnShardId.alternateString() + ENTITYINDEX_STORE_SUFFIX, ".armor");
    temps.add(entityIndexTemp);
    int read = 0;
    if (compressed > 0) {
      byte[] compressedIndex = new byte[compressed];
      read = IOTools.readFully(inputStream, compressedIndex, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedIndex, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(decompressed)) {
        Files.copy(bais, entityIndexTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      entityIndexWriter = new EntityIndexWriter(entityIndexTemp, columnShardId);
    } else {
      byte[] uncompressedIndex = new byte[uncompressed];
      read = IOTools.readFully(inputStream, uncompressedIndex, 0, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(uncompressedIndex)) {
        Files.copy(bais, entityIndexTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      entityIndexWriter = new EntityIndexWriter(entityIndexTemp, columnShardId);
    }
    return read;
  }
  
  private int loadRowGroup(DataInputStream inputStream, int compressed, int uncompressed, List<Path> temps) throws IOException {
    Path rgGroupTemp = TempFileUtil.createTempFile(columnShardId.alternateString() + ROWGROUP_STORE_SUFFIX, ".armor");
    temps.add(rgGroupTemp);
    int read = 0;
    if (compressed > 0) {
      byte[] compressedRg = new byte[compressed];
      read = IOTools.readFully(inputStream, compressedRg, 0, compressed);
      byte[] decompressed = Zstd.decompress(compressedRg, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(decompressed)) {
        Files.copy(bais, rgGroupTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      decompressed = null;
      rowGroupWriter = new RowGroupWriter(rgGroupTemp, columnShardId, valueDictionary);
      rowGroupWriter.position(rowGroupWriter.getCurrentSize());
    } else {
      byte[] uncompressedRg = new byte[uncompressed];
      read = IOTools.readFully(inputStream, uncompressedRg, 0, uncompressed);
      try (ByteArrayInputStream bais = new ByteArrayInputStream(uncompressedRg)) {
        Files.copy(bais, rgGroupTemp, StandardCopyOption.REPLACE_EXISTING);
      }
      rowGroupWriter = new RowGroupWriter(rgGroupTemp, columnShardId, valueDictionary);
      rowGroupWriter.position(rowGroupWriter.getCurrentSize());
    }
    return read;
  }

  // Read from the input streams to setup the writer.
  private void load(DataInputStream inputStream) throws IOException {
    ColumnFileReader cfr = new ColumnFileReader();
    List<Path> tempPaths = new ArrayList<>();
    boolean success = false;
    try {
      cfr.read(inputStream, (section, metadata, is, compressed, uncompressed) -> {
        try {
          if (section == ColumnFileSection.ENTITY_DICTIONARY) {
            return loadEntityDictionary(is, compressed, uncompressed);
          } else if (section == ColumnFileSection.VALUE_DICTIONARY) {
            return loadValueDictionary(is, compressed, uncompressed);
          } else if (section == ColumnFileSection.ENTITY_INDEX) {
            return loadEntityIndex(is, compressed, uncompressed, tempPaths);
          } else if (section == ColumnFileSection.ROWGROUP) {
            return loadRowGroup(inputStream, compressed, uncompressed, tempPaths);
          } else
            return 0;
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
   * 
   * @return A list of entity record summaries.
   */
  public List<EntityRecordSummary> getEntityRecordSummaries() {
    int byteLength = metadata.getColumnType().getByteLength();
    List<EntityRecord> records = entityIndexWriter.getEntityRecords(entityDictionary);
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
      for (EntityRecord er : records) {
        if (entityDictionary.getValue(er.getEntityId()) == null) {
          throw new RuntimeException("No string entity id exists for " + er.toString() + " in " + columnShardId.alternateString());
        }
      }

      return records.stream()
          .map(e -> new EntityRecordSummary(
              entityDictionary.getValue(e.getEntityId()),
              e.getValueLength() / byteLength, e.getRowGroupOffset(),
              e.getVersion(),
              e.getInstanceId()))
          .filter(e -> e.getNumRows() > 0).collect(Collectors.toList());
    }
  }

  public StreamProduct buildInputStream(Compression compress) throws IOException {
    return buildInputStream(compress, DEFAULT_VERSION);
  }

  public StreamProduct buildInputStream(Compression compress, Constants.ColumnFileFormatVersion version) throws IOException {
    switch (version) {
      case VERSION_1:
        throw new IllegalArgumentException("Column file format version " + version + " is no longer supported for writing");
      case VERSION_2:
        return buildInputStreamV2(compress);
      default:
        throw new IllegalArgumentException("Unknown column file format version " + version);
    }
  }

  static class SectionBuilder {
    private final ColumnFileSection sectionType;
    private ByteArrayOutputStream outputStream;

    public ByteArrayOutputStream getOutputStream() {
      return this.outputStream;
    }

    public SectionBuilder(ColumnFileSection header)
    {
      this.sectionType = header;
      this.outputStream = new ByteArrayOutputStream();
    }

    public Section buildWithLength()
       throws IOException
    {
      byte[] body = this.outputStream.toByteArray();
      byte[] len = writeLength(0, body.length);
      return new Section(this.sectionType, new ByteArraySubSection(len), new ByteArraySubSection(body));
    }

    public Section build() {
      return new Section(this.sectionType, this.outputStream.toByteArray());
    }
  }

  static interface SubSection
  {
    int getLength();
    InputStream getInputStream();
  }

  static class ByteArraySubSection implements SubSection
  {
    private byte[] byteArray;

    public ByteArraySubSection(byte[] ba)
    {
      this.byteArray = ba;
    }

    @Override public int getLength()
    {
      return byteArray.length;
    }

    @Override public InputStream getInputStream()
    {
      return new ByteArrayInputStream(this.byteArray);
    }
  }

  static class InputStreamSubSection implements SubSection
  {

    private final int length;
    private final InputStream inputStream;

    public InputStreamSubSection(InputStream s, int totalLength)
    {
      this.length = totalLength;
      this.inputStream = s;
    }

    @Override public int getLength()
    {
      return length;
    }

    @Override public InputStream getInputStream()
    {
      return this.inputStream;
    }
  }

  static class Section
  {
    private final ColumnFileSection type;
    SubSection lengthSubSection; // optional
    SubSection bodySubSection;

    public Section(ColumnFileSection type, byte[] ba)
    {
      this.type = type;
      this.bodySubSection = new ByteArraySubSection(ba);
    }

    public Section(ColumnFileSection type, SubSection ba, SubSection ba2)
    {
      this.type = type;
      this.lengthSubSection = ba;
      this.bodySubSection = ba2;
    }

    public List<SubSection> subSections()
    {
      if (lengthSubSection != null) {
        return Arrays.asList(this.lengthSubSection, this.bodySubSection);
      } else {
        return Arrays.asList(this.bodySubSection);
      }
    }

    public int totalSize()
    {
      int result = 0;
      if (this.lengthSubSection != null) {
        result += this.lengthSubSection.getLength();
      }
      result += this.bodySubSection.getLength();
      return result;
    }

    public ColumnFileSection getType() {
      return this.type;
    }
  }

  static class SectionMapper {

    private Section headerSection;

    private Map<ColumnFileSection, Section> sections;
    private List<Section> sectionsList;

    public SectionMapper() {
      sections = new HashMap<>();
      sectionsList = new ArrayList<>();
    }

    public List<Section> getAll()
       throws IOException
    {
      List<Section> result = new ArrayList<>();
      result.add(headerSection);
      result.add(computeTableOfContents(sectionsList));
      result.addAll(sectionsList);
      return result;
    }

    private Section computeTableOfContents(List<Section> sl)
       throws IOException
    {
      SectionBuilder headerPortion = new SectionBuilder(ColumnFileSection.HEADER);
      ByteArrayOutputStream os = headerPortion.getOutputStream();
      // number of records in the table of contents. each record is 8 bytes in size
      // considered alternative was to record in # of bytes, or *8, but was not taken
      //os.write(IOTools.toByteArray(sl.size()));
      int offset = 0;
      for( Section s : sl) {
        os.write(IOTools.toByteArray(s.getType().getSectionID()));
        os.write(IOTools.toByteArray(offset));
        offset += s.totalSize();
      }
      return headerPortion.buildWithLength();
    }

    public void add(Section section)
    {
      if (section.getType() == ColumnFileSection.HEADER) {
        headerSection = section;
      } else
      {
        sections.put(section.getType(), section);
        sectionsList.add(section);
      }
    }
  }

  public StreamProduct buildInputStreamV2(Compression compress) throws IOException {
    int totalBytes = 0;

    SectionMapper sections = new SectionMapper();

    Section headerPortion = getHeaderSection(Constants.ColumnFileFormatVersion.VERSION_2);
    sections.add(headerPortion);

    Section metadataSection = writeMetadata(compress);
    sections.add(metadataSection);

    // Send entity dictionary
    List<Path> tempPaths = new ArrayList<>();
    //totalBytes += 8;
    boolean success = false;
    try {
      sections.add(computeEntityDictionarySection(compress, tempPaths));
      sections.add(computeValueDictionarySection(compress, tempPaths));
      sections.add(computeEntityIndexSection(compress, tempPaths));
      sections.add(computeRowGroupSection(compress, tempPaths));

      ArrayList<InputStream> sequenceInputStreams = new ArrayList<>();

      for (Section sect : sections.getAll()) {
        for (SubSection blob : sect.subSections()) {
          totalBytes += blob.getLength();
          sequenceInputStreams.add(blob.getInputStream());
        }
      }

      StreamProduct product = new StreamProduct(totalBytes, new SequenceInputStream(Collections.enumeration(sequenceInputStreams)));
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

  private Section computeRowGroupSection(Compression compress, List<Path> tempPaths)
     throws IOException
  {
    return computeSectionCompressible(compress, tempPaths, "rowgroup-temp_",
       ColumnFileSection.ROWGROUP, rowGroupWriter, null, columnShardId.alternateString());
  }

  private Section compressToTempFile(ColumnFileSection sectionType, List<Path> tempPaths, String tempFileName, Component component, String tempName)
     throws IOException
  {
    Path tempPath = TempFileUtil.createTempFile(tempFileName + tempName + "-", ".armor");
    tempPaths.add(tempPath);
    try (ZstdOutputStream zstdOutput = new ZstdOutputStream(new FileOutputStream(tempPath
       .toFile()), RecyclingBufferPool.INSTANCE); InputStream rgInputStream = component.getInputStream())
    {
      IOTools.copy(rgInputStream, zstdOutput);
    }
    int payloadSize = (int)Files.size(tempPath);
    return new Section(sectionType,
       new ByteArraySubSection(writeLength((int)Files.size(tempPath), (int)component.getCurrentSize())),
       new InputStreamSubSection(new AutoDeleteFileInputStream(tempPath), (int)Files.size(tempPath)));
  }

  private Section computeEntityIndexSection(Compression compress, List<Path> tempPaths)
     throws IOException
  {
    int uncompressed = (int)entityIndexWriter.getCurrentSize();
    if (uncompressed % Constants.RECORD_SIZE_BYTES != 0)
    {
      int bytesOff = uncompressed % Constants.RECORD_SIZE_BYTES;
      LOGGER.error(
         "The entity index size {} is not in expected fixed width of {}. It is {} bytes off. Preload offset {}: See {}",
         uncompressed,
         Constants.RECORD_SIZE_BYTES,
         bytesOff,
         entityIndexWriter.getPreLoadOffset(),
         columnShardId.toSimpleString());
      throw new EntityIndexVariableWidthException(Constants.RECORD_SIZE_BYTES, uncompressed, bytesOff, entityIndexWriter.getPreLoadOffset(), columnShardId.toSimpleString());
    }
    return computeSectionCompressible(compress, tempPaths, "entity-temp_",
       ColumnFileSection.ENTITY_INDEX, entityIndexWriter, null, columnShardId.alternateString());
  }

  private <T extends Component> Section computeSectionCompressible(
     Compression compress, List<Path> tempPaths, String tempPrefix, ColumnFileSection sectionType, T component, Predicate<T> isEmptyPredicate, String shardTempName)
     throws IOException
  {
    Section section;

    if (isEmptyPredicate != null && isEmptyPredicate.test(component))
    {
      section = new Section(sectionType,
         new ByteArraySubSection(writeLength(0, 0)),
         new ByteArraySubSection(new byte[0]));
    }
    else if (compress == Compression.ZSTD)
    {
      section = compressToTempFile(sectionType, tempPaths, tempPrefix, component, shardTempName);
    }
    else
    {
      section = new Section(sectionType,
         new ByteArraySubSection(writeLength(0, (int)component.getCurrentSize())),
         new InputStreamSubSection(component.getInputStream(), (int)component.getCurrentSize()));
    }
    return section;
  }


  private Section computeValueDictionarySection(Compression compress, List<Path> tempPaths)
     throws IOException
  {
    return computeSectionCompressible(compress, tempPaths, "value-dict-temp_",
       ColumnFileSection.VALUE_DICTIONARY, valueDictionary, x -> x == null, columnShardId.alternateString());
  }

  private Section computeEntityDictionarySection(Compression compress, List<Path> tempPaths)
     throws IOException
  {
    return computeSectionCompressible(compress, tempPaths, "entity-dict-temp_",
       ColumnFileSection.ENTITY_DICTIONARY, entityDictionary, x -> x.isEmpty(), columnShardId.toSimpleString());
  }

  private Section getHeaderSection(Constants.ColumnFileFormatVersion version)
     throws IOException
  {
    SectionBuilder headerPortion = new SectionBuilder(ColumnFileSection.HEADER);
    writeForMagicHeader(headerPortion.getOutputStream());
    writeForVersion(headerPortion.getOutputStream(), version);
    return headerPortion.build();
  }

  private Section writeMetadata(Compression compress)
     throws IOException
  {
    SectionBuilder metadataPortion = new SectionBuilder(ColumnFileSection.METADATA);
    // Prepare metadata for writing
    metadata.setLastUpdate(new Date().toString());
    if (compress == Compression.ZSTD)
      metadata.setCompressionAlgorithm(Compression.ZSTD.name()); // Currently we only support this.
    else
      metadata.setCompressionAlgorithm(Compression.NONE.name());

    // TODO - should we have compacted the entities first?

    List<EntityRecord> records = entityIndexWriter.getEntityRecords(entityDictionary);
    entityIndexWriter.runThroughRecords(metadata, records);
    // Run through the values to update metadata
    if (!skipMetaData) {
      rowGroupWriter.runThoughValues(metadata, records);
    } else {
      metadata.setMaxValue(null);
      metadata.setMinValue(null);
    }
    // Store metadata
    String metadataStr = OBJECT_MAPPER.writeValueAsString(metadata);
    byte[] metadataPayload = metadataStr.getBytes();
    //writeLength(metadataPortion.getOutputStream(), 0, metadataPayload.length);
    metadataPortion.getOutputStream().write(metadataPayload);
    return metadataPortion.buildWithLength();
  }

  private static byte[] writeLength(int compressed, int uncompressed) throws IOException {
    byte[] lengths = new byte[8];
    System.arraycopy(IOTools.toByteArray(compressed), 0, lengths, 0, 4);
    System.arraycopy(IOTools.toByteArray(uncompressed), 0, lengths, 4, 4);
    return lengths;
  }

  private void writeLength(OutputStream outputStream, int compressed, int uncompressed) throws IOException {
    outputStream.write(IOTools.toByteArray(compressed));
    outputStream.write(IOTools.toByteArray(uncompressed));
  }

  public synchronized boolean delete(String transaction, Object entity, long version, String instanceId) {
    int entityId;
    boolean hasStringIds = !entityDictionary.isEmpty();
    if (entity instanceof String) {
      entityId = entityDictionary.getSurrogate((String) entity);
      entityDictionary.markForDeleted(entityId);
    } else if (entity instanceof Long) {
      if (hasStringIds)
        throw new EntityIdTypeException("Exepected a string type for the entity id but got a numeric type");
      entityId = ((Long) entity).intValue();
    } else if (entity instanceof Integer) {
      if (hasStringIds)
        throw new EntityIdTypeException("Exepected a string type for the entity id but got a numeric type");
      entityId = ((Integer) entity);
    } else
      throw new EntityIdTypeException("The entity type of " + entity.getClass().toString() + " is not supported for identity on entites");
    return delete(transaction, entityId, version, instanceId);
  }

  private synchronized boolean delete(String transaction, int entity, long version, String instanceId) {
    try {
      return entityIndexWriter.delete(entity, version, instanceId) != null;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public Integer getEntityId(Object entity) {
    Objects.requireNonNull(entity,"The entity parameter cannot be null");
    Integer entityInt = null;
    boolean hasStringIds = !entityDictionary.isEmpty();
    if (entity instanceof String) {
      entityInt = entityDictionary.getSurrogate((String) entity);
    } else if (entity instanceof Long) {
      if (hasStringIds)
        throw new EntityIdTypeException("Exepected a string type for the entity id but got a numeric type");
      entityInt = ((Long) entity).intValue();
    } else if (entity instanceof Integer) {
      if (hasStringIds)
        throw new EntityIdTypeException("Exepected a string type for the entity id but got a numeric type");
      entityInt = ((Integer) entity);
    } else
      throw new EntityIdTypeException("Entity uuids must be string, long or int not " + entity.getClass().getCanonicalName());
    return entityInt;
  }

  public synchronized void write(String transaction, List<WriteRequest> writeRequests) throws IOException {
    // First filter out already old stuff that doesn't need to be written.
    LinkedHashMap<Object, WriteRequest> groupByMax = new LinkedHashMap<>(); // NOTE: Maintain write order via a linkedhashmap or can't guarantee baseline/entity column is same as other.
    for (WriteRequest wr : writeRequests) {
      WriteRequest maxVersionWriteRequest = groupByMax.get(wr.getEntityId());
      if (maxVersionWriteRequest == null) {
        EntityRecord er = entityIndexWriter.getEntityRecord(getEntityId(wr.getEntityId()));
        if (er == null || er.getVersion() <= wr.getVersion())
          groupByMax.put(wr.getEntityId(), wr);
        else
          LOGGER.info("WARNING: Test we are skipping {} since its version {} is less than {}", er.getEntityId(), wr.getVersion(), er.getVersion());
      } else {
        if (maxVersionWriteRequest.getVersion() <= wr.getVersion())
          groupByMax.put(wr.getEntityId(), wr);
        else
          LOGGER.info("WARNING: Test we are skipping {} since its version {} is less than {}", wr.getEntityId(), wr.getVersion(), maxVersionWriteRequest.getVersion());
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
      entityIndexWriter.putEntity(er);
    }
  }

  private void writeForMagicHeader(OutputStream outputStream) throws IOException {
    outputStream.write(IOTools.toByteArray(MAGIC_HEADER));
  }
  
  private void writeForVersion(OutputStream outputStream, Constants.ColumnFileFormatVersion version) throws IOException {
    outputStream.write(IOTools.toByteArray(version.getVal()));
  }

  public void compact() throws IOException {
    compact(getEntityRecordSummaries());
  }

  // Compaction requires a list of entities to use, it can either be preexisting or non-existing.
  public void compact(List<EntityRecordSummary> entitiesToKeep) throws IOException {
	Instant mark = Instant.now();
    List<EntityRecord> entityRecords = new ArrayList<>();
    for (EntityRecordSummary entityCheck : entitiesToKeep) {
      final Integer entityId;
      if (entityCheck.getId() instanceof String) {
        entityId = entityDictionary.getSurrogate((String) entityCheck.getId());
        if (entityId == null)
          throw new RuntimeException("No surrogate could be found for " + entityCheck.getId());
      } else {
        entityId = (Integer) entityCheck.getId();
      }

      EntityRecord eRecord = entityIndexWriter.getEntityRecord(entityId);
      if (eRecord == null) {
        // This doesn't exist on this column but may on other columns according to the entities
        // passed in as a parameter. Because of this we should fill in null values for this.
        int valueLength = metadata.getColumnType().determineByteLength(entityCheck.getNumRows());
        EntityRecord er = new EntityRecord(
            entityId,
            Constants.NULL_FILLER_INDICATOR, // Trigger null fill in row group
            valueLength,
            entityCheck.getVersion(),
            (byte) 0,
            0,
            valueLength,
            entityCheck.getInstanceId() == null ? null : entityCheck.getInstanceId().getBytes());
        entityRecords.add(er);
      } else {
        entityRecords.add(eRecord);
      }
    }

    // With a list of sorted records, lets start the process of compaction
    List<EntityRecord> adjustedRecords = rowGroupWriter.compact(entityRecords);
    entityIndexWriter.compact(adjustedRecords);

    // Now hard-deleted deleted entites from entity index writer.
    Set<Integer> deletedEntities = 
        entityIndexWriter.getEntities().values().stream().filter(e -> e.getDeleted() == 1).map(e -> e.getEntityId()).collect(Collectors.toSet());
    entityIndexWriter.removeEntityReferences(deletedEntities);
    if (entityDictionary != null) {
      for (Integer surrogate : deletedEntities) {
        entityDictionary.getValue(surrogate);
      }
    }

    metadata.setLastCompactionDuration(Duration.between(mark, Instant.now()).toString());
    metadata.setLastCompaction(new Date().toString());
  }
}
