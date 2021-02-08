package com.rapid7.armor.entity;

import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SerDeTest {

  @Test
  public void columnIdSerDer() throws IOException {
    ObjectMapper om = new ObjectMapper();
    ColumnId cn = new ColumnId("a", DataType.STRING.getCode());

    String columnIdStr = om.writeValueAsString(cn);
    ColumnId testColumnId = om.readValue(columnIdStr, ColumnId.class);
    assertEquals(cn, testColumnId);
  }

  @Test
  public void columnSerDer() throws IOException {
    ObjectMapper om = new ObjectMapper();
    Column column = new Column(new ColumnId("a", DataType.STRING.getCode()));
    column.addValue("test123");
    column.addValue(null);

    String columnStr = om.writeValueAsString(column);
    Column testColumn = om.readValue(columnStr, Column.class);
    assertEquals(column, testColumn);
  }

  @Test
  public void rowSerDer() throws IOException {
    ObjectMapper om = new ObjectMapper();
    Row row = new Row();
    row.setColumns(Arrays.asList(null, 1, 2, 3));
    String rowStr = om.writeValueAsString(row);
    Row testRow = om.readValue(rowStr, Row.class);
    assertEquals(row, testRow);
  }

  @Test
  public void entitySerDer() throws IOException {
    ObjectMapper om = new ObjectMapper();
    Entity entity = new Entity();
    entity.setColumnIds(Arrays.asList(
        new ColumnId("a", DataType.STRING.getCode()),
        new ColumnId("b", DataType.INTEGER.getCode()),
        new ColumnId("c", DataType.INTEGER.getCode()),
        new ColumnId("d", DataType.INTEGER.getCode())));
    entity.setEntityIdColumn("austin");
    entity.setVersion(33);
    Row row1 = new Row();
    row1.setColumns(Arrays.asList(null, 1, 2, 3));

    Row row2 = new Row();
    row2.setColumns(Arrays.asList(1, 2, 3, null));
    entity.setRows(Arrays.asList(row1, row2));

    String entityStr = om.writeValueAsString(entity);
    Entity testEntity = om.readValue(entityStr, Entity.class);
    assertEquals(entity, testEntity);
  }
}
