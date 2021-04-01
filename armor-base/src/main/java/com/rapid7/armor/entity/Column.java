package com.rapid7.armor.entity;

import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Column {
  private ColumnId columnId;
  private List<Object> values = new ArrayList<>();
  private boolean returnNull = true;

  public void returnNull(boolean returnNull) {
    this.returnNull = returnNull;
  }
  public Column() {}

  public Column(ColumnId columnId) {
    this.columnId = columnId;
  }
  
  public int size() {
    return values.size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Column column = (Column) o;
    return Objects.equals(getColumnId(), column.getColumnId()) && Objects.equals(getValues(), column.getValues());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumnId(), getValues());
  }

  public List<Object> getValues() {
    return this.values;
  }

  public void setValues(List<Object> values) {
    this.values = values;
  }

  public ColumnId getColumnId() {
    return columnId;
  }

  public void setColumnId(ColumnId columnId) {
    this.columnId = columnId;
  }

  public void addValue(Object value) {
    checkType(value);
    values.add(value);
  }

  public Object[] values() {
    if (values == null || values.isEmpty()) {
      if (returnNull)
        return new Object[] {null};    // Never return zero rows always have one row.
      else
        return new Object[] {};
    }
    return values.toArray();
  }

  public int decodedByteLength() {
    if (columnId.dataType() == DataType.STRING)
      return values.stream().mapToInt(v -> v.toString().getBytes().length).sum();
    else
      return columnId.dataType().determineByteLength(values.size());
  }

  private void checkType(Object value) {
    if (value == null)
      return;
    switch (columnId.dataType()) {
      case BOOLEAN:
        if (value instanceof Boolean)
          return;
        break;
      case DATETIME:
      case LONG:
        if (value instanceof Long)
          return;
        break;
      case FLOAT:
        if (value instanceof Float)
          return;
        break;
      case DOUBLE:
        if (value instanceof Double)
          return;
        break;
      case STRING:
        if (value instanceof String)
          return;
        break;
      case INTEGER:
        if (value instanceof Integer)
          return;
        break;
    }
    throw new RuntimeException("The value of type " + value.getClass() + " doesn't match for this column type " + columnId.dataType());
  }
}
