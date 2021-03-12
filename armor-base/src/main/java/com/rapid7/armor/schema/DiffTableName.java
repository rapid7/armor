package com.rapid7.armor.schema;

import com.rapid7.armor.interval.Interval;

public final class DiffTableName {

  private DiffTableName() {}
  
  public static String generatePlusTableDiffName(String table, Interval interval, ColumnId columnId) {
    return table + "_" + interval.name().toLowerCase() + "_" + columnId.getName() + "_plus";
  }
  
  public static String generateMinusTableDiffName(String table, Interval interval, ColumnId columnId) {
    return table + "_" + interval.name().toLowerCase() + "_" + columnId.getName() + "_minus";
  }
}
