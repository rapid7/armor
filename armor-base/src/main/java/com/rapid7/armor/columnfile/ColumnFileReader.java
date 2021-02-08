package com.rapid7.armor.columnfile;

import static com.rapid7.armor.Constants.MAGIC_HEADER;

import java.io.DataInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;

public class ColumnFileReader {
  private ColumnMetadata metadata;
  
  public ColumnMetadata getColumnMetadata() {
    return metadata;
  }
  
  private int shouldHaveRead(int compressed, int uncompressed) {
    if (compressed > 0)
      return compressed;
    else
      return uncompressed;
  }

  // Assists in loading of a calling file by passing by streams to the caller, it is up to the caller to correctly
  // consume the stream and store the contents how they see it to be fit.
  public void read(DataInputStream dataInputStream, ColumnFileListener listener) throws IOException {
    // Header first
    readForMagicHeader(dataInputStream);

    // Metadata
    dataInputStream.readInt(); // Skip compressed, always uncompressed for meta
    int metadataLength = dataInputStream.readInt(); // Skip compressed, always uncompressed for meta
    byte[] metadataBytes = new byte[metadataLength];
    dataInputStream.readFully(metadataBytes);
    metadata = new ObjectMapper().readValue(metadataBytes, ColumnMetadata.class);
    // Load entity dictionary
    int entityDictCompressed = dataInputStream.readInt();
    int entityDictOriginal = dataInputStream.readInt();
    int readBytes = listener.columnFileSection(
       ColumnFileSection.ENTITY_DICTIONARY, metadata, dataInputStream, entityDictCompressed, entityDictOriginal);
    int shouldHaveRead = shouldHaveRead(entityDictCompressed, entityDictOriginal);
    if (readBytes < shouldHaveRead) {
       int remainingBytes = shouldHaveRead - readBytes;
       IOTools.skipFully(dataInputStream, remainingBytes);
    }
    int valueDictCompressed = dataInputStream.readInt();
    int valueDictOriginal = dataInputStream.readInt();
    readBytes = listener.columnFileSection(ColumnFileSection.VALUE_DICTIONARY, metadata, dataInputStream, valueDictCompressed, valueDictOriginal);
    shouldHaveRead = shouldHaveRead(valueDictCompressed, valueDictOriginal);
    if (readBytes < shouldHaveRead) {
      int remainingBytes = shouldHaveRead - readBytes;
      IOTools.skipFully(dataInputStream, remainingBytes);
    }
    int eiCompressed = dataInputStream.readInt();
    int eiOriginal = dataInputStream.readInt();
    readBytes = listener.columnFileSection(ColumnFileSection.ENTITY_INDEX, metadata, dataInputStream, eiCompressed, eiOriginal);
    shouldHaveRead = shouldHaveRead(eiCompressed, eiOriginal);
    if (readBytes < shouldHaveRead) {
      int remainingBytes = shouldHaveRead - readBytes;
      IOTools.skipFully(dataInputStream, remainingBytes);
    }
    int rgCompressed = dataInputStream.readInt();
    int rgOriginal = dataInputStream.readInt();
    readBytes = listener.columnFileSection(ColumnFileSection.ROWGROUP, metadata, dataInputStream, rgCompressed, rgOriginal);
    shouldHaveRead = shouldHaveRead(rgCompressed, rgOriginal);
    if (readBytes < shouldHaveRead) {
      int remainingBytes = shouldHaveRead - readBytes;
      IOTools.skipFully(dataInputStream, remainingBytes);
    }
  }

  private void readForMagicHeader(DataInputStream dataInputStream) throws IOException {
    short header = dataInputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("The magic header doesn't exist");
  }
}
