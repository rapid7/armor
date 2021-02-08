package com.rapid7.armor.columnfile;

import java.io.DataInputStream;

import com.rapid7.armor.meta.ColumnMetadata;

@FunctionalInterface
public interface ColumnFileListener {
  int columnFileSection(
      ColumnFileSection armorSection, ColumnMetadata metadata, DataInputStream inputStream, int compressedLength, int uncompressedLength);
}
