package com.rapid7.armor.meta;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;

@JsonInclude(value = Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetadata {
  private String entityColumnId;
  private String entityColumnIdType;
  private Set<ColumnId> columnIds = new HashSet<>();
  private String tenant;
  private String table;

  public TableMetadata() {}

  public TableMetadata(String tenant, String table, String entityColumnId, String entityColumnIdType) {
    this.entityColumnId = entityColumnId;
    this.entityColumnIdType = entityColumnIdType;
    this.table = table;
    this.tenant = tenant;
  }

  public void setTenant(String tenant) {
    this.tenant = tenant;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getTenant() {
    return tenant;
  }
  
  public String getTable() {
    return table;
  }

  public String getEntityColumnId() {
    return entityColumnId;
  }

  public void setEntityColumnId(String entityColumnId) {
    this.entityColumnId = entityColumnId;
  }

  public DataType entityIdColumnType() {
    return DataType.getDataType(entityColumnIdType);
  }

  public String getEntityColumnIdType() {
    return entityColumnIdType;
  }

  public void setEntityColumnIdType(String entityColumnIdType) {
    this.entityColumnIdType = entityColumnIdType;
  }
  
  public void addColumnIds(Collection<ColumnId> columnIds) {
    this.columnIds.addAll(columnIds);
  }

  public void setColumnIds(Set<ColumnId> columnIds) {
    this.columnIds = columnIds;
  }

  public Set<ColumnId> getColumnIds() {
     return columnIds;
  }
}
