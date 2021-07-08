package com.rapid7.armor.columnfile;

public enum ColumnFileSection {
  HEADER,
  TOC, // only in V2
  METADATA,
  ENTITY_DICTIONARY,
  VALUE_DICTIONARY,
  ENTITY_INDEX,
  ROWGROUP
}
