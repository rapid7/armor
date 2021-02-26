package com.rapid7.armor.meta;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;

public class MetadataTest {
    private ObjectMapper om = new ObjectMapper();

    @Test
    public void metadataTest() throws IOException {
      TableMetadata tmd = new TableMetadata("columnId", "columnIdType");
      ShardMetadata smd1 = new ShardMetadata();
      
      ColumnMetadata cmd1 = new ColumnMetadata();
      cmd1.setColumnName("column1");
      cmd1.setColumnType(DataType.STRING);
      
      ColumnMetadata cmd2 = new ColumnMetadata();
      cmd2.setColumnName("column2");
      cmd2.setColumnType(DataType.STRING);
      
      smd1.setColumnMetadata(Arrays.asList(cmd1, cmd2));
      ColumnMetadata cmd3 = new ColumnMetadata();
      cmd3.setColumnName("column3");
      cmd3.setColumnType(DataType.INTEGER);
      
      ShardMetadata smd2 = new ShardMetadata();
      smd2.setColumnMetadata(Arrays.asList(cmd3));
      tmd.setShardMetadata(Arrays.asList(smd1, smd2));

      Set<ColumnId> expectedColumnIds = Sets.newHashSet(new ColumnId("column1", "S"), new ColumnId("column2", "S"), new ColumnId("column3", "I"));
      assertEquals(expectedColumnIds, new HashSet<>(tmd.getColumnIds()));
      String payload = om.writeValueAsString(tmd);
      TableMetadata tmd2 = om.readValue(payload, TableMetadata.class);
    }
}
