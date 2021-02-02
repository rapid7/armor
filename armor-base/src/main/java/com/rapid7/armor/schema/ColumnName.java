package com.rapid7.armor.schema;

import java.util.Objects;

public class ColumnName {
  public final static String SEPERATOR = "_";
  private String name;
  private String type;

  public ColumnName() {
  }

  public ColumnName(String fullName) {
    String[] parts = fullName.split(SEPERATOR);
    this.name = parts[0];
    DataType dt = DataType.getDataType(parts[1]);
    this.type = dt.getCode();  
  }

  public ColumnName(String name, String type) {
    this.name = name;
    this.type = type;
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
    return "ColumnName{" +
        "name='" + name + '\'' +
        ", type='" + type + '\'' +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ColumnName that = (ColumnName) o;
    return Objects.equals(getName(), that.getName()) && Objects.equals(getType(), that.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getType());
  }
}
