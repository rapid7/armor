package com.rapid7.armor;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.Row;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.fast.FastArmorBlock;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.slow.SlowArmorReader;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import com.rapid7.armor.store.FileWriteStore;
import com.rapid7.armor.write.ArmorWriter;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import tech.tablesaw.columns.Column;
import tech.tablesaw.filtering.*;
import org.junit.jupiter.api.Test;

public class FileStoreTestV2 {

  // The table schema we will be working with
  private static List<ColumnName> COLUMNS = Arrays.asList(
    new ColumnName("status", DataType.INTEGER.getCode()),
    new ColumnName("time", DataType.LONG.getCode()),
    new ColumnName("vuln", DataType.STRING.getCode()));
  
  // List of vuln we will be working with by states
  private static Row texasVuln = new Row(1, 101l, "texas");
  private static Row caliVuln = new Row(2, 102l, "cali");
  private static Row zonaVuln = new Row(3, 103l, "zona");
  private static Row nyVuln = new Row(4, 104l, "ny");
  private static Row nevadaVuln = new Row(5, 105l, "nevada");
  private static Row oregonVuln = new Row(6, 106l, "oregon");
  private static Row utahVuln = new Row(7, 107l, "utah");

  private static final String TENANT = "united_states";
  private static final String TABLE = "state_vulns";
  private static String TEST_UUID = UUID.randomUUID().toString();
  private static final String ASSET_ID = "assetId";
  private static final Random RANDOM = new Random();
  
  private void checkEntityIndexRecord(EntityRecord eir, int rowGroupOffset, int valueLength, int nullLength, byte deleted) {
    assertEquals(valueLength, eir.getValueLength());
    assertEquals(rowGroupOffset, eir.getRowGroupOffset());
    assertEquals(nullLength, eir.getNullLength());
    assertEquals(deleted, eir.getDeleted());
  }
  
  private Entity generateEntity(String entityId, long version, Row... rows) {
    return Entity.buildEntity(ASSET_ID, entityId, version, TEST_UUID, COLUMNS, rows);
  }
  
  private Entity generateEntity(Integer entityId, long version, Row... rows) {
    return Entity.buildEntity(ASSET_ID, entityId, version, TEST_UUID, COLUMNS, rows);
  }
  
  private Entity randomEntity(long version, Row... rows) {
    return Entity.buildEntity(ASSET_ID, UUID.randomUUID().toString(), version, TEST_UUID, COLUMNS, rows);
  }

  private void removeDirectory(Path removeDirectory) throws IOException {
    Files.walk(removeDirectory).filter(Files::isRegularFile).map(Path::toFile).forEach(File::delete);
    Files.walk(removeDirectory)
      .sorted(Comparator.reverseOrder())
      .map(Path::toFile)
      .filter(File::isDirectory)
      .forEach(File::delete);
  }
  
  private Table entityToTableSawRow(Entity entity) {
    Table smallTable = Table.create("");
    for (com.rapid7.armor.entity.Column column : entity.columns()) {
      ColumnName columnName = column.getColumnName();
      if (DataType.STRING == columnName.dataType()) {
        List<String> strValues = column.getValues().stream().map(v -> v == null ? null : v.toString()).collect(Collectors.toList());
        smallTable.addColumns(StringColumn.create(columnName.getName(), strValues));
      } else if (DataType.INTEGER == columnName.dataType()) {
        int[] intValues = new int[column.size()];
        List<Object> values = column.getValues();
        for (int i = 0; i < column.size(); i++) {
          if (values.get(i) == null) {
            intValues[i] = Integer.MIN_VALUE;
          } else {
            intValues[i] = (Integer) values.get(i);
          }
        }
        smallTable.addColumns(IntColumn.create(columnName.getName(), intValues));
      } else if (DataType.LONG == columnName.dataType()) {
        long[] longValues = new long[column.size()];
        List<Object> values = column.getValues();
        for (int i = 0; i < column.size(); i++) {
          if (values.get(i) == null) {
            longValues[i] = Long.MIN_VALUE;
          } else {
            longValues[i] = (Long) values.get(i);
          }
        }
        smallTable.addColumns(LongColumn.create(columnName.getName(), longValues));
      }
    }
    int rowCount = smallTable.rowCount();
    if (entity.getEntityId() instanceof String) {
      List<String> values = IntStream.range(0, rowCount).mapToObj(i -> entity.getEntityId().toString()).collect(Collectors.toList());
      smallTable.addColumns(StringColumn.create(entity.getEntityIdColumn(), values));
    } else if (entity.getEntityId() instanceof Integer) {
      int[] array = new int[rowCount];
      Arrays.fill(array, (Integer) entity.getEntityId());
      smallTable.addColumns(IntColumn.create(entity.getEntityIdColumn(), array));
    } else if (entity.getEntityId() instanceof Long) {
      long[] array = new long[rowCount];
      Arrays.fill(array, (Long) entity.getEntityId());
      smallTable.addColumns(LongColumn.create(entity.getEntityIdColumn(), array));
    }
    return smallTable;
  }

  private void assertTableEquals(Table a, Table b) {
    assertEquals(a.rowCount(), b.rowCount());
    assertEquals(a.columnCount(), b.columnCount());
    for (int i = 0; i < a.rowCount(); i++) {
      tech.tablesaw.api.Row aRow = a.row(i);
      tech.tablesaw.api.Row bRow = b.row(i);
      for (int ii = 0; ii < aRow.columnCount(); ii++) {
        assertEquals(aRow.getObject(ii), bRow.getObject(ii));
      }
    }
  }
  
  private void verifyEntityDeletedReaderPOV(Entity entity, Path path) {
    FileReadStore readStore = new FileReadStore(path);
    SlowArmorReader reader = new SlowArmorReader(readStore);
    Table entityTable = reader.getEntity(TENANT, TABLE, entity.getEntityId());
    assertEquals(0, entityTable.rowCount());
  }
  
  private void verifyEntityReaderPOV(Entity entity, Path path) {
    FileReadStore readStore = new FileReadStore(path);
    Table checkEntity = entityToTableSawRow(entity);
    SlowArmorReader reader = new SlowArmorReader(readStore);
    Table entityTable = reader.getEntity(TENANT, TABLE, entity.getEntityId());
    entityTable = entityTable.sortAscendingOn("vuln").select("assetId", "vuln", "time", "status");
    checkEntity = checkEntity.sortAscendingOn("vuln").select("assetId", "vuln", "time", "status");
    assertTableEquals(checkEntity, entityTable);
  }
  
  private void verifyTableReaderPOV(int expectedNumberRows, Path path, int numShards) {
    FileReadStore readStore = new FileReadStore(path);
    List<ShardId> shardIds = readStore.findShardIds(TENANT, TABLE);
    assertEquals(numShards, shardIds.size());
    int totalRows = 0;
    for (ShardId shardId : shardIds) {
      Integer shardRows = null;
      for (ColumnName columnName : COLUMNS) {
        FastArmorShardColumn fas = readStore.getFastArmorShard(shardId, columnName.getName());
        FastArmorBlockReader far = fas.getFastArmorColumnReader();
        DataType dt = fas.getDataType();
        FastArmorBlock fab = null;
        switch (dt) {
        case INTEGER:
          fab = far.getIntegerBlock(5000);
          break;
        case LONG:
          fab = far.getLongBlock(5000);
          break;
        case STRING:
          fab = far.getStringBlock(5000);
          break;
          default:
            throw new RuntimeException("Unsupported");
        }
        
        if (shardRows == null) {
          shardRows = fab.getNumRows();
        } else if (shardRows != fab.getNumRows()) {
          throw new RuntimeException("Within a shard the two column row counts do not match");
        }
      }
      totalRows += shardRows;
    }
    assertEquals(expectedNumberRows, totalRows);
  }
  
  @Test
  public void verifySameXactError() {
    
  }
  
  @Test
  public void burstCheck() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    System.out.println("Test directory at " + testDirectory);
    int numShards = 10;
    int numEntities = 1000;
    Row[] rows = new Row[] {texasVuln, caliVuln};
    try {
      // Test with 10 shards
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
      
      ArmorWriter writer = new ArmorWriter("aw1", store, true, numShards, null, null);
      String xact = writer.startTransaction();
      List<Entity> entities = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        Entity random = generateEntity(i, 1, rows);
        entities.add(random);
      }
      
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities));
      writer.save(xact, TENANT, TABLE);
      writer.close();
      
      verifyTableReaderPOV(numEntities*2, testDirectory, numShards);
      int random = RANDOM.nextInt(999);
      verifyEntityReaderPOV(entities.get(random), testDirectory);
      
      // Now lets delete them all
      writer = new ArmorWriter("aw1", store, true, numShards, null, null);
      xact = writer.startTransaction();
      for (int i = 0; i < 1000; i++) {
        writer.delete(xact, TENANT, TABLE, i);
      }
      writer.save(xact, TENANT, TABLE);
      writer.close();
      
      verifyTableReaderPOV(0, testDirectory, numShards);
      verifyEntityDeletedReaderPOV(entities.get(random), testDirectory);
      
      // Add it back
      writer = new ArmorWriter("aw1", store, true, numShards, null, null);
      xact = writer.startTransaction();      
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities));
      writer.save(xact, TENANT, TABLE);
      
      verifyTableReaderPOV(numEntities*2, testDirectory, numShards);
      random = RANDOM.nextInt(999);
      verifyEntityReaderPOV(entities.get(random), testDirectory);
      
      // NOTE: Notice we didn't close the writer yet! Add another 1K.
      List<Entity> entities1 = new ArrayList<>();
      for (int i = 1000; i < 2000; i++) {
        Entity random1 = generateEntity(i, 1, rows);
        entities1.add(random1);
      }

      xact = writer.startTransaction();
      // Attempt to also try and double count it shouldn't double count.
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities1));
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities1));

      writer.save(xact, TENANT, TABLE);
      
      verifyTableReaderPOV((2*numEntities)*2, testDirectory, numShards);
      random = RANDOM.nextInt(999);
      verifyEntityReaderPOV(entities.get(random), testDirectory);
      verifyEntityReaderPOV(entities1.get(random), testDirectory);
      writer.close();
      
      // Finally lets add more entites to the table in 2 valid batch before we save and finish this test.
      writer = new ArmorWriter("aw1", store, true, numShards, null, null);
      xact = writer.startTransaction();      
      List<Entity> entities2 = new ArrayList<>();
      for (int i = 2000; i < 3000; i++) {
        Entity random2 = generateEntity(i, 1, rows);
        entities2.add(random2);
      }

      List<Entity> entities3 = new ArrayList<>();
      for (int i = 3000; i < 4000; i++) {
        Entity random3 = generateEntity(i, 1, rows);
        entities3.add(random3);
      }
      
      // Make it out of order with respect to the enityIds.
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities3));
      writer.write(xact, TENANT, TABLE, new ArrayList<>(entities2));
      writer.save(xact, TENANT, TABLE);
      verifyTableReaderPOV((4*numEntities)*2, testDirectory, numShards);
      random = RANDOM.nextInt(999);
      verifyEntityReaderPOV(entities.get(random), testDirectory);
      verifyEntityReaderPOV(entities1.get(random), testDirectory);
      verifyEntityReaderPOV(entities2.get(random), testDirectory);
      verifyEntityReaderPOV(entities3.get(random), testDirectory);
      writer.close();
      
    } finally {
      removeDirectory(testDirectory);
    }
  }
}
