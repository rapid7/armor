package com.rapid7.armor.write.component;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.write.EntityOffsetException;
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
import java.util.stream.Collectors;
import static com.rapid7.armor.Constants.BEGIN_DELETE_OFFSET;
import static com.rapid7.armor.Constants.RECORD_SIZE_BYTES;

/**
 * Writer for entity indexes. Write calls are written into memory (heap or direct) first and can be flushed to target upon
 * commit or close. Target can be a path or a given output stream.
 */
public class EntityRecordWriter extends FileComponent {
  private Map<Integer, EntityRecord> entities = new HashMap<>();
  private Map<Integer, Integer> indexOffsets = new HashMap<>();
  private final ColumnShardId columnShardId;
  private int nextOffset = 0;
  private final ByteBuffer readByteBuffer = ByteBuffer.allocate(RECORD_SIZE_BYTES);
  private final ByteBuffer writeByteBuffer = ByteBuffer.allocate(RECORD_SIZE_BYTES);

  public EntityRecordWriter(Path path, ColumnShardId columnShardId) throws IOException {
    super(path);
    this.columnShardId = columnShardId;
    nextOffset = (int) getCurrentSize();
    if (nextOffset > 0) {
      loadOffsets();
    }
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
      for (int i = 0; i < nextOffset; i += RECORD_SIZE_BYTES) {
        rawRecords.add(readEntityIndexRecord());
      }
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
    metadata.setNumRows(metadata.getDataType().rowCount(usedBytes));
  }

  public EntityRecord delete(int entityUuid) throws IOException {
    if (entities.containsKey(entityUuid)) {
      long prevPosition = position();
      try {
        int indexOffset = indexOffsets.get(entityUuid);
        position(indexOffset + BEGIN_DELETE_OFFSET);
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put((byte) 1);
        buffer.flip();
        write(buffer);
        EntityRecord eir = entities.get(entityUuid);
        eir.setDeleted((byte) 1);
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
    for (int i = 0; i < nextOffset; i += RECORD_SIZE_BYTES) {
      EntityRecord eir = readEntityIndexRecord();
      entities.put(eir.getEntityId(), eir);
      indexOffsets.put(eir.getEntityId(), i);
    }
  }

  protected EntityRecord readEntityIndexRecord() throws IOException {
    readByteBuffer.rewind();
    read(readByteBuffer);
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


  public void writeEntityIndexRecord(EntityRecord eir) throws IOException {
    writeEntityRecordToBuffer(eir, writeByteBuffer);
    write(writeByteBuffer);
  }

  /**
   * Defrag here is taking a given list of records that should be recorded. Callers should filter out
   * records that do not need to be recorded (delete). The approach here is to build a temp file and then replace
   * the underlying file as well as in-memory records with new ones.
   */
  public void defrag(List<EntityRecord> entityRecords) throws IOException {
    Map<Integer, EntityRecord> tempEntities = new HashMap<>();
    Map<Integer, Integer> tempIndexOffsets = new HashMap<>();
    ByteBuffer buffer = ByteBuffer.allocate(RECORD_SIZE_BYTES);
    Path path = Files.createTempFile("entityrecordwriter-defrag-" + columnShardId.alternateString() + "-", ".armor");
    boolean copied = false;
    try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
      int recordOffset = 0;
      for (EntityRecord er : entityRecords) {
        if (er.getDeleted() == 1)
          continue;
        tempEntities.put(er.getEntityId(), er);
        tempIndexOffsets.put(er.getEntityId(), recordOffset);
        writeEntityRecordToBuffer(er, buffer);
        fileChannel.write(buffer);
        recordOffset += RECORD_SIZE_BYTES;
      }
      copied = true;
    } finally {
      if (!copied)
        Files.deleteIfExists(path);
    }
    boolean rebased = false;
    try {
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
