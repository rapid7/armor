package com.rapid7.armor.columnfile;

import java.util.Arrays;

public enum ColumnFileSection {
  HEADER(-1),
  TOC(-1), // only in V2
  METADATA(1),
  ENTITY_DICTIONARY(2),
  VALUE_DICTIONARY(3),
  ENTITY_INDEX(4),
  ROWGROUP(5),
  VALUE_INDEX(6);

  private final int sectionID;

  ColumnFileSection(int i)
  {
    this.sectionID = i;
  }

  public static ColumnFileSection fromID(int sectionType)
  {
    return Arrays.stream(values()).filter(x -> x.sectionID == sectionType).findFirst().orElse(null);
  }

  public int getSectionID()
  {
    return sectionID;
  }
}
