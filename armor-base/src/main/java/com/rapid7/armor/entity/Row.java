package com.rapid7.armor.entity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Row {
  private List<Object> columns = new ArrayList<>();

  public Row() {
  }

  public Row(Object... values) {
    columns.addAll(Arrays.asList(values));
  }

  public static Row buildRow(Object... values) {
    return new Row(values);
  }

  public static List<Row> buildRows(int numColumns, Object... values) {
    if (values.length % numColumns != 0)
      throw new RuntimeException("The shape of the columns to values doesn't match up");
    List<Row> rows = new ArrayList<>();
    int columnIndex = 0;
    Row row = new Row();
    for (Object value : values) {
      row.addColumn(value);
      columnIndex++;
      if (columnIndex == numColumns) {
        rows.add(row);
        row = new Row();
        columnIndex = 0;
      }
    }
    return rows;
  }

  public static List<Row> buildRows(List<List<Object>> rows) {
    List<Row> toReturn = new ArrayList<>();
    for (List<Object> row : rows) {
      toReturn.add(new Row(row));
    }
    return toReturn;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Row row = (Row) o;
    return Objects.equals(getColumns(), row.getColumns());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getColumns());
  }

  @Override
  public String toString() {
    return "Row{" +
        "columns=" + columns + '}';
  }

  public int numColumns() {
    return columns.size();
  }

  public List<Object> getColumns() {
    return this.columns;
  }

  public void setColumns(List<Object> columns) {
    this.columns = columns;
  }

  public void addColumn(Object value) {
    columns.add(value);
  }

  public Object getValue(int columnIndex) {
    return columns.get(columnIndex);
  }
} 
