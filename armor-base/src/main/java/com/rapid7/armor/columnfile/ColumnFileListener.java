package com.rapid7.armor.columnfile;

import java.io.DataInputStream;

import com.rapid7.armor.ArmorSection;

@FunctionalInterface
public interface ColumnFileListener {
  int columnFileSection(ArmorSection armorSection, DataInputStream inputStream, int compressedLength, int uncompressedLength);
}
