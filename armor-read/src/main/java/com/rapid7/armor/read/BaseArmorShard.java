package com.rapid7.armor.read;

import static com.rapid7.armor.Constants.RECORD_SIZE_BYTES;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.github.luben.zstd.Zstd;
import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public abstract class BaseArmorShard {
  protected ColumnMetadata metadata;
  protected DictionaryReader strValueDictionary;
  protected DictionaryReader entityDictionaryReader;
  protected List<EntityRecord> entityRecords;

  protected abstract void readRowGroup(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata) throws IOException;
  
  protected void readValueDictionary(DataInputStream inputStream, int compressed, int uncompressed, ColumnMetadata metadata) throws IOException {
    // Read string value dictionary (if required)
    if (compressed > 0) {
      byte[] compressedIndex = new byte[compressed];
      IOTools.readFully(inputStream, compressedIndex, 0, compressed);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, uncompressed);
      strValueDictionary = new DictionaryReader(decomporessedIndex, metadata.getCardinality(), false);
    } else if (uncompressed > 0) {
      byte[] decompressed = new byte[uncompressed];
      IOTools.readFully(inputStream, decompressed, 0, uncompressed);
      strValueDictionary = new DictionaryReader(decompressed, metadata.getCardinality(), false);
    }
  }
  
  protected void readEntityIndex(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {
    if (compressed > 0) {
      byte[] compressedIndex = new byte[compressed];
      IOTools.readFully(inputStream, compressedIndex, 0, compressed);
      byte[] decompressedIndex = Zstd.decompress(compressedIndex, uncompressed);
      try (DataInputStream uncompressedInputStream = new DataInputStream(new ByteArrayInputStream(decompressedIndex))) {
        entityRecords = readAllIndexRecords(uncompressedInputStream, uncompressed);
      }
    } else {
      byte[] entityIndexBytes = new byte[uncompressed];
      int read = IOTools.readFully(inputStream, entityIndexBytes, 0, uncompressed);
      try (DataInputStream uncompressedInputStream = new DataInputStream(new ByteArrayInputStream(entityIndexBytes))) {
        entityRecords = readAllIndexRecords(uncompressedInputStream, uncompressed);
      }
    }
    entityRecords = EntityRecord.sortActiveRecordsByOffset(entityRecords);
  }

  protected void readEntityDictionary(DataInputStream inputStream, int compressed, int uncompressed) throws IOException {    
    if (compressed > 0) {
      byte[] compressedIndex = new byte[compressed];
      inputStream.read(compressedIndex);
      byte[] decomporessedIndex = Zstd.decompress(compressedIndex, uncompressed);
      entityDictionaryReader = new DictionaryReader(decomporessedIndex, metadata.getNumEntities(), true);
    } else if (uncompressed > 0) {
      byte[] decompressed = new byte[uncompressed];
      inputStream.read(decompressed);
      entityDictionaryReader = new DictionaryReader(decompressed, metadata.getNumEntities(), true);
    }
  }

  protected List<EntityRecord> readAllIndexRecords(DataInputStream inputStream, int originalLength) throws IOException {
    List<EntityRecord> records = new ArrayList<>();
    for (int i = 0; i < originalLength; i += RECORD_SIZE_BYTES) {
      EntityRecord eir = readIndexRecords(inputStream);
      if (eir.getDeleted() == 0)
        records.add(eir);
    }
    return records;
  }

  protected EntityRecord readIndexRecords(DataInputStream inputStream) throws IOException {
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
