package com.rapid7.armor.columnfile;

import static com.rapid7.armor.Constants.MAGIC_HEADER;

import java.io.DataInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.ArmorSection;
import com.rapid7.armor.meta.ColumnMetadata;

public class ColumnFileReader {
  private ColumnMetadata metadata;
  
  public ColumnMetadata getColumnMetadata() {
    return metadata;
  }

  // Assetis in loading of a calling file by passing by streams to the caller, it is up to the caller to correctly
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
    int readBytes = listener.columnFileSection(ArmorSection.ENTITY_DICTIONARY, dataInputStream, entityDictCompressed, entityDictOriginal);
    
    int valueDictCompressed = dataInputStream.readInt();
    int valueDictOriginal = dataInputStream.readInt();
    listener.columnFileSection(ArmorSection.VALUE_DICTIONARY, dataInputStream, valueDictCompressed, valueDictOriginal);

    int eiCompressed = dataInputStream.readInt();
    int eiOriginal = dataInputStream.readInt();
    listener.columnFileSection(ArmorSection.ENTITY_INDEX, dataInputStream, eiCompressed, eiOriginal);

    int rgCompressed = dataInputStream.readInt();
    int rgOriginal = dataInputStream.readInt();
    listener.columnFileSection(ArmorSection.ROWGROUP, dataInputStream, rgCompressed, rgOriginal);    
  }

  private void readForMagicHeader(DataInputStream dataInputStream) throws IOException {
    short header = dataInputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("The magic header doesn't exist");
  }
}
