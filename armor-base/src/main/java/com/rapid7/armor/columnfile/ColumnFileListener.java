package com.rapid7.armor.columnfile;

import java.io.DataInputStream;

import com.rapid7.armor.ArmorSection;

@FunctionalInterface
public interface ColumnFileListener {
  void columnFileSection(ArmorSection armorSection, DataInputStream inputStream, int compressedLength, int uncompressedLength);
}
