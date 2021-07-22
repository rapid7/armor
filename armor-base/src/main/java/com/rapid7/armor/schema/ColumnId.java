package com.rapid7.armor.schema;

import java.nio.file.Path;
import java.util.Objects;

public class ColumnId {
  public final static String SEPERATOR = "_";
  public final static String ENTITY_COLUMN_IDENTIFIER = "-";
  private String name;
  private String type;

  public ColumnId() {
  }

  public ColumnId(ColumnId columnId) {
    this.name = columnId.getName();
    this.type = columnId.getType();
  }

  public ColumnId(String fullName) {
    int lastUnderscore = fullName.lastIndexOf(SEPERATOR);
    this.name = fullName.substring(0, lastUnderscore);
    this.type = DataType.getDataType(fullName.substring(lastUnderscore + 1)).getCode();
  }
  
  public ColumnId(String name, DataType type) {
      this.name = name;
      this.type = type.getCode();
    }

  public ColumnId(String name, String type) {
    this.name = name;
    this.type = type;
  }

  public ColumnId(Path columnFile) {
    this(columnFile.getFileName().toString());
  }

  public DataType dataType() {
    return DataType.getDataType(type);
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String fullName() {
    return name + SEPERATOR + type;
  }
  
  public static String keyName(ColumnId columnId, boolean isEntity) {
    return (isEntity ? ENTITY_COLUMN_IDENTIFIER : SEPERATOR) + SEPERATOR + columnId.fullName();
  }

  @Override
  public String toString() {
    return "ColumnId{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnId that = (ColumnId) o;
    return Objects.equals(getName(), that.getName()) && Objects.equals(getType(), that.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getType());
  }
}
