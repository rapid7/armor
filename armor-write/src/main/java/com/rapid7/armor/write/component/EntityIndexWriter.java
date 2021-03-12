package com.rapid7.armor.write.component;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.FixedCapacityByteBufferPool;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.write.EntityOffsetException;

import static com.rapid7.armor.Constants.RECORD_SIZE_BYTES;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writer for entity indexes. Write calls are written into memory (heap or direct) first and can be flushed to target upon
 * commit or close. Target can be a path or a given output stream.
 */
public class EntityIndexWriter extends FileComponent {
  private static final Logger LOGGER = LoggerFactory.getLogger(EntityIndexWriter.class);

  private Map<Integer, EntityRecord> entities = new HashMap<>();
  private Map<Integer, Integer> indexOffsets = new HashMap<>();
  private final ColumnShardId columnShardId;
  private int nextOffset = 0;
  private int preloadOffset = 0;
  private final static FixedCapacityByteBufferPool BYTE_BUFFER_POOL = new FixedCapacityByteBufferPool(RECORD_SIZE_BYTES);
  private final static String ENTITY_INDEX_COMPACTION_SUFFIX = "_entityindex-compaction-";

  public static int bufferPoolSize() {
    return BYTE_BUFFER_POOL.currentSize();
  }

  public EntityIndexWriter(Path path, ColumnShardId columnShardId) throws IOException {
    super(path);
    this.columnShardId = columnShardId;
    nextOffset = (int) getCurrentSize();
    preloadOffset = nextOffset;
    int bytesOff = nextOffset % RECORD_SIZE_BYTES;
    if (bytesOff > 0) {
      if (bytesOff > RECORD_SIZE_BYTES) {
        throw new EntityIndexVariableWidthException(RECORD_SIZE_BYTES, nextOffset, bytesOff, nextOffset, columnShardId.alternateString());
      } else {
        LOGGER.error("The entity index is not of fixed record size {} bytes, total index is {} and is off by {} bytes. Some data could be lost see: {}",
            RECORD_SIZE_BYTES, nextOffset, bytesOff, columnShardId.alternateString());
        LOGGER.error("Readjusting by truncating next offset from {} to {}..see {}",
            nextOffset, nextOffset - bytesOff, columnShardId.alternateString());
        truncate(nextOffset-bytesOff);
        nextOffset = (int) getCurrentSize();
        preloadOffset = nextOffset;
      }
    }
    if (nextOffset > 0) {
      loadOffsets();
    }
  }
    
  public int getPreLoadOffset() {
    return preloadOffset;
  }

  public EntityRecord getEntityRecord(Integer entityId) {
    return entities.get(entityId);
  }

  public Map<Integer, EntityRecord> getEntities() {
    return entities;
  }

  public List<EntityRecord> getEntityRecords(DictionaryWriter dict) {
    if (dict == null || dict.isEmpty())
      return EntityRecord.sortRecordsByOffset(entities.values().stream().filter(e -> e.getDeleted() == 0).collect(Collectors.toList()));
    else
      return EntityRecord.sortRecordsByOffset(entities.values().stream().filter(e -> e.getDeleted() == 0).collect(Collectors.toList()), dict);
  }
  
  public List<EntityRecord> allRecords() throws IOException {
    long position = position();
    try {
      position(0);
      List<EntityRecord> rawRecords = new ArrayList<>();
      safeTraverse((a) -> {
        try {
          rawRecords.add(readEntityIndexRecord());
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      });
      return rawRecords;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      position(position);
    }
  }

  public void runThroughRecords(ColumnMetadata metadata, List<EntityRecord> entityRecords) {
    // Now traverse through noting bytes used and unused.
    int cursor = 0;
    long freeableBytes = 0;
    int usedBytes = 0;
    int numEntities = 0;
    for (EntityRecord er : entityRecords) {
      if (cursor < er.getRowGroupOffset()) {
        freeableBytes += er.getRowGroupOffset() - cursor;
        cursor = er.getRowGroupOffset();
        int totalLength = er.totalLength();
        if (er.getDeleted() == 1) {
          freeableBytes += totalLength;
          cursor += totalLength;
        } else {
          numEntities++;
          cursor += totalLength;
          usedBytes += er.getValueLength();
        }
      } else if (cursor == er.getRowGroupOffset()) {
        int totalLength = er.totalLength();
        if (er.getDeleted() == 1) {
          freeableBytes += totalLength;
          cursor += totalLength;
        } else {
          numEntities++;
          cursor += totalLength;
          usedBytes += er.getValueLength();
        }
      } else {
        throw new EntityOffsetException(columnShardId, cursor, er, entityRecords);
      }
    }
    metadata.setNumEntities(numEntities);
    float fragPercent = ((float) freeableBytes / (freeableBytes + usedBytes));
    metadata.setFragmentationLevel((int) (fragPercent * 100));
    metadata.setNumRows(metadata.getColumnType().rowCount(usedBytes));
  }

  public EntityRecord delete(int entityUuid, long version, String instanceId) throws IOException {
    // Before executing the delete, first ensure the version is higher or greater than.
    EntityRecord eir = entities.get(entityUuid);
    if (eir != null && version >= eir.getVersion()) {
      long prevPosition = position();
      int indexOffset = indexOffsets.get(entityUuid);
      try {
        position(indexOffset);
        eir.setDeleted((byte) 1);
        eir.instanceId(instanceId);
        eir.setVersion(version);
        writeEntityIndexRecord(eir);
        entities.put(entityUuid, eir);
        return eir;
      } finally {
        position(prevPosition);
      }
    }
    return null;
  }

  public boolean putEntity(EntityRecord eir) throws IOException {
    if (entities.containsKey(eir.getEntityId())) {
      int indexOffset = indexOffsets.get(eir.getEntityId());
      long prevPosition = position();
      try {
        position(indexOffset);
        writeEntityIndexRecord(eir);
      } finally {
        position(prevPosition);
      }
      entities.put(eir.getEntityId(), eir);
    } else {
      position(nextOffset);
      writeEntityIndexRecord(eir);
      entities.put(eir.getEntityId(), eir);
      indexOffsets.put(eir.getEntityId(), nextOffset);
      nextOffset += RECORD_SIZE_BYTES;
    }
    return true;
  }

  private void loadOffsets() throws IOException {
    safeTraverse((offset) -> {
      try {
        EntityRecord eir = readEntityIndexRecord();
        entities.put(eir.getEntityId(), eir);
        indexOffsets.put(eir.getEntityId(), offset);
      } catch (IOException ioe) {
        throw new RuntimeException("Detected an error loading index", ioe);
      }
    });
  }
  
  private void safeTraverse(Consumer<Integer> function) {
    for (int i = 0; i < nextOffset; i += RECORD_SIZE_BYTES) {
      // Do a check to see 
      if (i + RECORD_SIZE_BYTES > nextOffset) {
        int bytesOff = nextOffset - i;
        LOGGER.error("The entity index is not of fixed record size {} bytes, total index is {} and is off by {} bytes. Some data could be lost see: {}",
            RECORD_SIZE_BYTES, nextOffset, bytesOff, columnShardId.alternateString());
        LOGGER.error("Readjusting next offset from {} to {}..see {}",
            nextOffset, nextOffset - bytesOff, columnShardId.alternateString());
        nextOffset = nextOffset - bytesOff;
        break;
      }
      function.accept(i);
    }
  }

  protected EntityRecord readEntityIndexRecord() throws IOException {
    ByteBuffer readByteBuffer = BYTE_BUFFER_POOL.get();
    try {
      readByteBuffer.rewind();
      int byteRead = read(readByteBuffer);
      readByteBuffer.flip();
      int entityUuid = readByteBuffer.getInt();
      int offset = readByteBuffer.getInt();
      int valueLength = readByteBuffer.getInt();
      long version = readByteBuffer.getLong();
      byte deleted = readByteBuffer.get();
      int nullLength = readByteBuffer.getInt();
      int decodedLength = readByteBuffer.getInt();
      byte[] instanceid = new byte[Constants.INSTANCE_ID_BYTE_LENGTH];
      readByteBuffer.get(instanceid, 0, Constants.INSTANCE_ID_BYTE_LENGTH);
      return new EntityRecord(entityUuid, offset, valueLength, version, deleted, nullLength, decodedLength, instanceid);
    } finally {
      BYTE_BUFFER_POOL.release(readByteBuffer);
    }
  }

  private void writeEntityRecordToBuffer(EntityRecord eir, ByteBuffer byteBuffer) throws IOException {
    byteBuffer.clear();
    byteBuffer.putInt(eir.getEntityId());
    byteBuffer.putInt(eir.getRowGroupOffset());
    byteBuffer.putInt(eir.getValueLength());
    byteBuffer.putLong(eir.getVersion());
    byteBuffer.put(eir.getDeleted());
    byteBuffer.putInt(eir.getNullLength());
    byteBuffer.putInt(eir.getDecodedLength());
    byteBuffer.put(eir.getInstanceId());
    byteBuffer.flip();
  }

  public void removeEntityReferences(Set<Integer> toRemove) {
    for (Integer entityId : toRemove) {
      entities.remove(entityId);
      indexOffsets.remove(entityId);
    }
  }

  public void writeEntityIndexRecord(EntityRecord eir) throws IOException {
    ByteBuffer writeByteBuffer = BYTE_BUFFER_POOL.get();
    try {
      writeEntityRecordToBuffer(eir, writeByteBuffer);
      write(writeByteBuffer);
    } finally {
      BYTE_BUFFER_POOL.release(writeByteBuffer);
    }
  }

  /**
   * Compacts here is taking a given list of records that should be recorded. Callers should filter out
   * records that do not need to be recorded (delete). The approach here is to build a temp file and then replace
   * the underlying file as well as in-memory records with new ones.
   * 
   * @param entitiesToKeep A list of entity records to keep during compaction.
   */
  public void compact(List<EntityRecord> entitiesToKeep) throws IOException {
    Map<Integer, EntityRecord> tempEntities = new HashMap<>();
    Map<Integer, Integer> tempIndexOffsets = new HashMap<>();
    Path path = Files.createTempFile(columnShardId.alternateString() + ENTITY_INDEX_COMPACTION_SUFFIX, ".armor");
    boolean copied = false;
    ByteBuffer buffer = BYTE_BUFFER_POOL.get();
    try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      int recordOffset = 0;
      for (EntityRecord er : entitiesToKeep) {
        if (er.getDeleted() == 1) {
          continue;
        }
        tempEntities.put(er.getEntityId(), er);
        tempIndexOffsets.put(er.getEntityId(), recordOffset);
        writeEntityRecordToBuffer(er, buffer);
        int written = fileChannel.write(buffer);
        if (written != RECORD_SIZE_BYTES)
          throw new RuntimeException("When compacting, only write " + written + " when it should have been " + RECORD_SIZE_BYTES);
        recordOffset += RECORD_SIZE_BYTES;
      }
      copied = true;
    } finally {
      BYTE_BUFFER_POOL.release(buffer);
      if (!copied)
        Files.deleteIfExists(path);
    }
    boolean rebased = false;
    int totalSize = (int) Files.size(path);
    try {
      if (totalSize % RECORD_SIZE_BYTES != 0)
        throw new RuntimeException("When compacting, the fixed page size of " + RECORD_SIZE_BYTES + " wasn't achieved: " + totalSize);
      rebase(path);
      rebased = true;
    } finally {
      if (!rebased)
        Files.deleteIfExists(path);
    }
    nextOffset = (int) Files.size(path);
    entities = tempEntities;
    indexOffsets = tempIndexOffsets;
  }
}
