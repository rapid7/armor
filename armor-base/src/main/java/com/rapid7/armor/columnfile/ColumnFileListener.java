package com.rapid7.armor.columnfile;

import java.io.DataInputStream;

import com.rapid7.armor.meta.ColumnMetadata;

@FunctionalInterface
public interface ColumnFileListener {
  /**
   * Listener callback. For each section of the column file other than the metadata, this will be called.
   * @param armorSection file section type
   * @param metadata metadata information for this column file.
   * @param inputStream data to be read.
   * @param compressedLength if the data is compressed, this value will be > 0. If uncompressed, value will be 0.
   * @param uncompressedLength the uncompressed length of the data. if compressedLength is 0, this is the number of bytes available to read.
   * @return number of bytes read by this function from inputStream . For backward compatibility, function should return 0 bytes for unhandled @{ColumnFileSection}s, such as unknown types.
   */
  int columnFileSection(
      ColumnFileSection armorSection, ColumnMetadata metadata, DataInputStream inputStream, int compressedLength, int uncompressedLength);
}
