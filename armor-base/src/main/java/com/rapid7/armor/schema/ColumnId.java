package com.rapid7.armor.schema;

import java.nio.file.Path;
import java.util.Objects;

public class ColumnId {
  public final static String SEPERATOR = "_";
  private String name;
  private String type;

  public ColumnId() {
  }

  public ColumnId(String fullName) {
    String[] parts = fullName.split(SEPERATOR);
    this.name = parts[0];
    DataType dt = DataType.getDataType(parts[parts.length - 1]);
    this.type = dt.getCode();  
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
