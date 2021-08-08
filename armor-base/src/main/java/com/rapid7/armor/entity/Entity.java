package com.rapid7.armor.entity;

import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class Entity {
  private List<Row> rows = new ArrayList<>();
  private List<ColumnId> columnIds = new ArrayList<>();
  private String entityIdColumn;
  private Object entityId;
  private long version;
  private String instanceid;
  private boolean columnsDefined = false;
  private boolean rowsAdded = false;
  public Entity() {}

  public Entity(String entityIdColumn, Object entityId, long version, String instanceid, ColumnId... columnIds) {
    this.entityIdColumn = entityIdColumn;
    this.entityId = verifyIdType(entityId);
    this.version = version;
    this.columnIds = Arrays.asList(columnIds);
    this.columnsDefined = true;
    this.instanceid = instanceid;
    checkEntityIdColumn();
  }

  public Entity(String entityIdColumn, Object entityId, long version, String instanceid, List<ColumnId> columnIds) {
    this.entityIdColumn = entityIdColumn;
    this.entityId = verifyIdType(entityId);
    this.version = version;
    this.columnIds = columnIds;
    this.instanceid = instanceid;
    this.columnsDefined = true;

    checkEntityIdColumn();
  }

  public Entity(String entityIdColumn, Object entityId, long version, String instanceid, List<ColumnId> columnIds, List<Row> rows) {
    this.entityIdColumn = entityIdColumn;
    this.entityId = verifyIdType(entityId);
    this.version = version;
    this.columnIds = columnIds;
    this.columnsDefined = true;
    validateRows(rows);
    this.rows = rows;
    this.rowsAdded = true;
    this.instanceid = instanceid;

    checkEntityIdColumn();
  }

  public Entity(String entityIdColumn, Object entityId, long version, String instanceid, List<ColumnId> columnIds, Row... rows) {
    this.entityIdColumn = entityIdColumn;
    this.entityId = verifyIdType(entityId);
    this.version = version;
    this.columnIds = columnIds;
    this.columnsDefined = true;
    if (rows == null || rows.length == 0)
      this.rows = new ArrayList<>();
    else
      this.rows = Arrays.asList(rows);
    validateRows(this.rows);
    this.rowsAdded = true;
    this.instanceid = instanceid;
    checkEntityIdColumn();
  }
  
  public ColumnId entityColumnId() {
      return new ColumnId(entityIdColumn, DataType.inferDataType(entityId));
  }

  private Object verifyIdType(Object entityId) {
    if (entityId instanceof String)
      return entityId;
    if (entityId instanceof Long)
      return entityId;
    if (entityId instanceof Integer)
      return entityId;
    throw new RuntimeException("Invalid class type for entity");
  }
  

  public static Entity buildEntity(String entityIdColumn, Object entityId, long version, String instanceId, List<ColumnId> columnIds, Row... rows) {
    return new Entity(entityIdColumn, entityId, version, instanceId, columnIds, rows);
  }

  public static Entity buildEntity(String entityIdName, Object entityId, long version, String instanceId, List<ColumnId> columnIds, List<Row> rows) {
    return new Entity(entityIdName, entityId, version, instanceId, columnIds, rows);
  }

  public static Entity buildEntity(String entityIdName, Object entityId, long version, String instanceId, ColumnId... columnIds) {
    return new Entity(entityIdName, entityId, version, instanceId, columnIds);
  }

  public static Entity buildEntity(String entityIdName, Object entityId, long version, String instanceId, List<ColumnId> columnIds) {
    return new Entity(entityIdName, entityId, version, instanceId, columnIds);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Entity entity = (Entity) o;
    return getVersion() == entity.getVersion() && Objects.equals(getRows(), entity.getRows()) && Objects.equals(getColumnIds(), entity.getColumnIds()) && Objects.equals(getEntityIdColumn(), entity.getEntityIdColumn()) && Objects.equals(getEntityId(), entity.getEntityId()) && Objects.equals(instanceid, entity.instanceid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRows(), getColumnIds(), getEntityIdColumn(), getEntityId(), getVersion(), instanceid);
  }

  private void checkEntityIdColumn() {
    if (columnIds.stream().anyMatch(c -> c.getName().equalsIgnoreCase(entityIdColumn)))
      throw new RuntimeException("You defined a column with the same name as entityIdColumn, remove the that column definition it is not needed");
  }
  
  private void validateRows(List<Row> rows) {
    if (!columnsDefined)
      return;
    int numColumns = this.columnIds.size();
    for (Row row : rows) {
      if (row == null)
        throw new RuntimeException("You cannot use a null value for row, you must create a row and set null values");
      if (row.numColumns() != numColumns) {
        throw new RuntimeException("The row has " + row + " has " + row.numColumns() + " columns when definition is " + columnIds);
      }
      // TODO: Verify input matches schema
    }
  }

  public List<Row> getRows() {
    return this.rows;
  }

  public void setRows(List<Row> rows) {
    validateRows(rows);
    this.rows = rows;
    this.rowsAdded = true;
  }

  public void addRow(Row row) {
    validateRows(Arrays.asList(row));
    rows.add(row);
    this.rowsAdded = true;
  }

  public void addRows(List<Row> rows) {
    validateRows(rows);
    this.rows.addAll(rows);
    this.rowsAdded = true;
  }

  public void addRow(Object... values) {
    Row row = Row.buildRow(values);
    validateRows(Arrays.asList(row));
    rows.add(row);
    this.rowsAdded = true;
  }

  public void addRows(Object... values) {
    List<Row> rowsInput = Row.buildRows(columnIds.size(), values);
    validateRows(rowsInput);
    rows.addAll(rowsInput);
    this.rowsAdded = true;
  }

  public Column getColumn(int columnNum) {
    Column column = new Column(columnIds.get(columnNum - 1));
    for (Row row : rows) {
      column.addValue(row.getValue(columnNum - 1));
    }
    return column;
  }

  public int numRows() {
    return rows.size();
  }

  public Object getValue(int rowNum, int columnNum) {
    Row row = rows.get(rowNum - 1);
    return row.getValue(columnNum - 1);
  }

  public Row row(int rowNum) {
    return rows.get(rowNum - 1);
  }

  public List<ColumnId> getColumnIds() {
    return this.columnIds;
  }

  public void setColumnIds(List<ColumnId> columnIds) {
    this.columnIds = columnIds;
    this.columnsDefined = true;
    if (rowsAdded)
      validateRows(rows);
  }

  public String getEntityIdColumn() {
    return entityIdColumn;
  }

  public void setEntityIdColumn(String entityIdColumn) {
    this.entityIdColumn = entityIdColumn;
  }

  public Object getEntityId() {
    return entityId;
  }

  public void setEntityId(Object entityId) {
    this.entityId = entityId;
  }

  public String getInstanceId() {
    return instanceid;
  }

  public void setInstanceId(String instanctId) {
    this.instanceid = instanctId;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public List<Column> columns() {
    List<Column> columnValues = new ArrayList<>();
    for (ColumnId columnId : columnIds) {
      columnValues.add(new Column(columnId));
    }

    // Enforce every entity has some physical row
    if (rows == null || rows.isEmpty()) {
      for (Column cv : columnValues) {
        cv.addValue(null);
      }
    } else {
      for (Row row : rows) {
        for (int i = 0; i < columnValues.size(); i++) {
          Column cv = columnValues.get(i);
          cv.addValue(row.getValue(i));
        }
      }
    }
    return columnValues;
  }
}
