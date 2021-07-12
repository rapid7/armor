package com.rapid7.armor.columnfile;

import static com.rapid7.armor.Constants.MAGIC_HEADER;
import static com.rapid7.armor.Constants.VERSION;

import java.io.DataInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import com.rapid7.armor.Constants;
import com.rapid7.armor.io.IOTools;
import com.rapid7.armor.meta.ColumnMetadata;

public class ColumnFileReader {
  private ColumnMetadata metadata;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

    Constants.ColumnFileFormatVersion version = readForFormatVersion(dataInputStream);

    switch (version) {
      case VERSION_1:
        readV1(dataInputStream, listener);
        break;
      case VERSION_2:
        readV2(dataInputStream, listener);
        break;
      default:
        throw new IllegalArgumentException("Unable to read columnfile since version is " + version + " which is unsupported");
    }
  }

  static class TableOfContentsEntry {
    ColumnFileSection sectionType;
    int offset;
    TableOfContentsEntry(ColumnFileSection sectionType, int sectionOffset) {
      this.sectionType = sectionType;
      this.offset = sectionOffset;
    }
    static TableOfContentsEntry create(int sectionType, int sectionOffset) {
      ColumnFileSection st = ColumnFileSection.fromID(sectionType);
      if (st != null) {
        return new TableOfContentsEntry(st, sectionOffset);
      } else {
        throw new IllegalArgumentException("Columnfile contains unknown section type " + sectionType + " in table of contents");
      }
    }
  }

  private List<TableOfContentsEntry> readTableOfContents(DataInputStream dataInputStream)
     throws IOException
  {
    List<TableOfContentsEntry> result = new ArrayList<>();
    int recordCount = dataInputStream.readInt();
    for (int i = 0; i < recordCount; ++i) {
      int sectionType = dataInputStream.readInt();
      int sectionOffset = dataInputStream.readInt();
      result.add(TableOfContentsEntry.create(sectionType, sectionOffset));
    }
    return result;
  }

  private void readV2(DataInputStream dataInputStream, ColumnFileListener listener) throws IOException {
    // Table of Contents
    List<TableOfContentsEntry> toc = readTableOfContents(dataInputStream);

    // read sections in their order from the TOC
    int totalBytesRead = 0;
    for (TableOfContentsEntry entry : toc) {
      if (entry.offset != totalBytesRead) {
        // should be warning?
        throw new IllegalArgumentException("Columnfile byte offset does not match table of contents for section " + entry.sectionType + " at offset " + entry.offset + " comparison to " + totalBytesRead);
      }

      int readBytes = 0;
      if (entry.sectionType == ColumnFileSection.METADATA) {
        readBytes = readMetadata(dataInputStream);
      } else
      {
        readBytes = readSection(dataInputStream, listener, entry.sectionType);
      }
      totalBytesRead += readBytes;
    }
  }

  private int readSection(DataInputStream dataInputStream, ColumnFileListener listener, ColumnFileSection valueDictionary)
     throws IOException
  {
    int readBytes = 0;
    int compressedSize = dataInputStream.readInt();
    int uncompressedSize = dataInputStream.readInt();
    if (listener != null)
    {
      readBytes = listener.columnFileSection(valueDictionary, metadata, dataInputStream, compressedSize, uncompressedSize);
    }
    int shouldHaveRead = shouldHaveRead(compressedSize, uncompressedSize);
    if (readBytes < shouldHaveRead)
    {
      int remainingBytes = shouldHaveRead - readBytes;
      IOTools.skipFully(dataInputStream, remainingBytes);
    }
    return shouldHaveRead + 8; // for the 2 readInts for the compressed / uncompressed sizes
  }

  private int readMetadata(DataInputStream dataInputStream)
     throws IOException
  {
    dataInputStream.readInt(); // Skip compressed, always uncompressed for meta
    int metadataLength = dataInputStream.readInt(); // Skip compressed, always uncompressed for meta
    byte[] metadataBytes = new byte[metadataLength];
    dataInputStream.readFully(metadataBytes);
    metadata = OBJECT_MAPPER.readValue(metadataBytes, ColumnMetadata.class);
    return metadataLength + 8; // for the first two readInts for the compressed / uncompressed sizes
  }

  private void readV1(DataInputStream dataInputStream, ColumnFileListener listener) throws IOException {
    // Metadata
    readMetadata(dataInputStream);
    // Load entity dictionary
    readSection(dataInputStream, listener, ColumnFileSection.ENTITY_DICTIONARY);
    readSection(dataInputStream, listener, ColumnFileSection.VALUE_DICTIONARY);
    readSection(dataInputStream, listener, ColumnFileSection.ENTITY_INDEX);
    readSection(dataInputStream, listener, ColumnFileSection.ROWGROUP);
  }

  private void readForMagicHeader(DataInputStream dataInputStream) throws IOException {
    short header = dataInputStream.readShort();
    if (header != MAGIC_HEADER)
      throw new IllegalArgumentException("The magic header doesn't exist");
  }
  
  private Constants.ColumnFileFormatVersion readForFormatVersion(DataInputStream dataInputStream) throws IOException {
    int version = dataInputStream.readInt();

    for (Constants.ColumnFileFormatVersion v: Constants.ColumnFileFormatVersion.values()) {
      if (v.getVal() == version) {
        return v;
      }
    }
    throw new IllegalArgumentException("Unable to read columnfile since version is " + version + " and this lib is only for " + VERSION);
  }
}
