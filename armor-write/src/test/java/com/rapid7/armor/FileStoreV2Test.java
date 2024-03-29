package com.rapid7.armor;

import static com.rapid7.armor.interval.Interval.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.Row;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.fast.FastArmorBlock;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.slow.SlowArmorReader;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import com.rapid7.armor.store.FileWriteStore;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.writers.ArmorWriter;
import com.rapid7.armor.write.writers.EntityIdTypeException;
import com.rapid7.armor.xact.XactError;

import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.LongColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

public class FileStoreV2Test {

  // The table schema we will be working with
  private static List<ColumnId> COLUMNS = Arrays.asList(
      new ColumnId("status", DataType.INTEGER.getCode()),
      new ColumnId("time", DataType.LONG.getCode()),
      new ColumnId("vuln", DataType.STRING.getCode()));

  // List of vuln we will be working with by states
  private static Row texasVuln = new Row(1, 101l, "texas");
  private static Row caliVuln = new Row(2, 102l, "cali");
  private static Row zonaVuln = new Row(3, 103l, "zona");
  private static Row nyVuln = new Row(4, 104l, "ny");
  private static Row nevadaVuln = new Row(5, 105l, "nevada");
  private static Row oregonVuln = new Row(6, 106l, "oregon");
  private static Row utahVuln = new Row(7, 107l, "utah");

  private static Row NULL_ROW = new Row(null, null, null);

  private static Row[] STATE_ROWS = new Row[] {texasVuln, caliVuln, zonaVuln, nyVuln, nevadaVuln, oregonVuln, utahVuln};

  private static final String TENANT = "united_states";
  private static final String TABLE = "state_vulns";
  private static final Interval INTERVAL = SINGLE;
  private static final Instant TIMESTAMP = Instant.now();
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

  private Entity randomEntity(long version, int numRows) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++)
      rows.add(generateRandomRow());
    return Entity.buildEntity(ASSET_ID, UUID.randomUUID().toString(), version, TEST_UUID, COLUMNS, rows);
  }

  private Row generateRandomRow() {
    int randomInt = RANDOM.nextInt();
    long randomLong = RANDOM.nextLong();
    byte[] array = new byte[7]; 
    RANDOM.nextBytes(array);
    String generatedString = new String(array, Charset.forName("UTF-8"));
    return new Row(randomInt, randomLong, generatedString);
  }

  private List<Row> generateRandomRowsFromSet() {
    int grab = RANDOM.nextInt(7);
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < grab; i++) {
      rows.add(STATE_ROWS[RANDOM.nextInt(7)]);
    }
    return rows;
  }
  
  private long numFilesInDirectory(Path directory) throws IOException {
    return Files.walk(directory)
        .parallel()
        .filter(p -> !p.toFile().isDirectory())
        .count();
  }

  private void removeDirectory(Path removeDirectory) throws IOException {
    try {
     Files.walk(removeDirectory).filter(Files::isRegularFile).map(Path::toFile).forEach(File::delete);
     Files.walk(removeDirectory)
     .sorted(Comparator.reverseOrder())
     .map(Path::toFile)
     .filter(File::isDirectory)
     .forEach(File::delete);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Table entityToTableSawRow(Entity entity) {
    Table smallTable = Table.create("");
    for (com.rapid7.armor.entity.Column column : entity.columns()) {
      ColumnId columnId = column.getColumnId();
      if (DataType.STRING == columnId.dataType()) {
        List<String> strValues = column.getValues().stream().map(v -> v == null ? null : v.toString()).collect(Collectors.toList());
        smallTable.addColumns(StringColumn.create(columnId.getName(), strValues));
      } else if (DataType.INTEGER == columnId.dataType()) {
        int[] intValues = new int[column.size()];
        List<Object> values = column.getValues();
        for (int i = 0; i < column.size(); i++) {
          if (values.get(i) == null) {
            intValues[i] = Integer.MIN_VALUE;
          } else {
            intValues[i] = (Integer) values.get(i);
          }
        }
        smallTable.addColumns(IntColumn.create(columnId.getName(), intValues));
      } else if (DataType.LONG == columnId.dataType()) {
        long[] longValues = new long[column.size()];
        List<Object> values = column.getValues();
        for (int i = 0; i < column.size(); i++) {
          if (values.get(i) == null) {
            longValues[i] = Long.MIN_VALUE;
          } else {
            longValues[i] = (Long) values.get(i);
          }
        }
        smallTable.addColumns(LongColumn.create(columnId.getName(), longValues));
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
    Table entityTable = reader.getEntity(TENANT, TABLE, INTERVAL, TIMESTAMP, entity.getEntityId());
    assertEquals(0, entityTable.rowCount());
  }

  private String printTable(Path path) {
    FileReadStore readStore = new FileReadStore(path);
    SlowArmorReader reader = new SlowArmorReader(readStore);
    Table entityTable = reader.getTable(TENANT, TABLE, INTERVAL, TIMESTAMP);
    return entityTable.print();
  }

  private void verifyEntityReaderPOV(Entity entity, Path path) {
    FileReadStore readStore = new FileReadStore(path);
    Table checkEntity = entityToTableSawRow(entity);
    SlowArmorReader reader = new SlowArmorReader(readStore);
    Table entityTable = reader.getEntity(TENANT, TABLE, INTERVAL, TIMESTAMP, entity.getEntityId());
    entityTable = entityTable.sortAscendingOn("vuln").select("assetId", "vuln", "time", "status");
    checkEntity = checkEntity.sortAscendingOn("vuln").select("assetId", "vuln", "time", "status");
    assertTableEquals(checkEntity, entityTable);
  }

  private void verifyColumn(int expectedNumberRows, ColumnId column, Path path, int numShards) throws IOException {
    FileReadStore readStore = new FileReadStore(path);
    List<ShardId> shardIds = readStore.findShardIds(TENANT, TABLE, INTERVAL, TIMESTAMP);
    assertEquals(numShards, shardIds.size());
    int totalRows = 0;
    FastArmorReader reader = new FastArmorReader(readStore);
    for (ShardId shardId : shardIds) {
      FastArmorBlockReader far = reader.getColumn(shardId, column.getName());
      FastArmorBlock fab = null;
      switch (column.dataType()) {
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
        throw new RuntimeException("Unsupported data type" + column);
      }
      totalRows += fab.getNumRows();
    }
    assertEquals(expectedNumberRows, totalRows);
  }

  private void verifyTableReaderPOV(
    int expectedNumberRows, String table, Interval interval, Instant timestamp, Path path, int numShards, Collection<ColumnId> columnIds) {
    FileReadStore readStore = new FileReadStore(path);
    List<ShardId> shardIds = readStore.findShardIds(TENANT, table, interval, timestamp);
    assertEquals(numShards, shardIds.size());
    int totalRows = 0;
    for (ShardId shardId : shardIds) {
      Integer shardRows = null;
      for (ColumnId columnId : columnIds) {
        FastArmorShardColumn fas = readStore.getFastArmorShard(shardId, columnId.getName());
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

  private void verifyTableReaderPOV(int expectedNumberRows, Path path, int numShards) {
    FileReadStore readStore = new FileReadStore(path);
    List<ShardId> shardIds = readStore.findShardIds(TENANT, TABLE, INTERVAL, TIMESTAMP);
    assertEquals(numShards, shardIds.size());
    int totalRows = 0;
    for (ShardId shardId : shardIds) {
      Integer shardRows = null;
      for (ColumnId columnId : COLUMNS) {
        FastArmorShardColumn fas = readStore.getFastArmorShard(shardId, columnId.getName());
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
  public void mixEntityIdTypeError() {
    Assertions.assertThrows(EntityIdTypeException.class, () -> {
      Path testDirectory = Files.createTempDirectory("filestore");
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(1));
      Row[] rows2 = new Row[] { texasVuln };
      Entity e1 = generateEntity("firstEntity", 1, rows2);
      Entity e2 = generateEntity(4, 2, rows2);
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 1)) {
        writer.begin();
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e1, e2));
      } finally {
        removeDirectory(testDirectory);       
      }
    });
    
    Assertions.assertThrows(EntityIdTypeException.class, () -> {
      Path testDirectory = Files.createTempDirectory("filestore");
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(1));
      Row[] rows2 = new Row[] { texasVuln };
      Entity e1 = generateEntity("firstEntity", 1, rows2);
      Entity e2 = generateEntity(4, 2, rows2);
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 1)) {
        writer.begin();
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e1));
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e2));
      } finally {
        removeDirectory(testDirectory);       
      }
    });
    
    Assertions.assertThrows(EntityIdTypeException.class, () -> {
      Path testDirectory = Files.createTempDirectory("filestore");
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(1));
      Row[] rows2 = new Row[] { texasVuln };
      Entity e1 = generateEntity("firstEntity", 1, rows2);
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 1)) {
        writer.begin();
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e1));
        Entity e = new Entity(ASSET_ID, 3, System.currentTimeMillis(), null);
        writer.delete(TENANT, TABLE, INTERVAL, TIMESTAMP, e);
      } finally {
        removeDirectory(testDirectory);       
      }
    });
  }

  @Test
  public void deleteOutOfOrder() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
    Row[] rows2 = new Row[] { texasVuln, caliVuln };
    Entity e1 = generateEntity("firstEntity", 1, rows2);
    Entity e2 = generateEntity("firstEntity", 2, rows2);

    try {
      for (int i = 0; i < 2; i++) {
        List<String> track = Collections.synchronizedList(new ArrayList<>());
        if (i == 1)
          RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
        for (Compression compression : Compression.values()) {
          try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10)) {
            writer.begin();
            track.add("going to write " + e1.getEntityId());
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e1));
            track.add("wrote " + e1.getEntityId());
            new Thread(new Runnable() {
              @Override
              public void run() {
                Entity e = new Entity(ASSET_ID, e1.getEntityId(), 3, "test");
                String pre = "going to delete " + e1.getEntityId();
                track.add(pre);
                writer.delete(TENANT, TABLE, INTERVAL, TIMESTAMP, e);
                String post = "deleted " + e1.getEntityId();
                track.add(post);
              }
            }).start();
            track.add("going to write " + e2.getEntityId());
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(e2));
            track.add("wrote " + e2.getEntityId());
            writer.commit();
          } catch (Exception e) {
            System.out.println("Detected an error, going to print out the order of writes/deletes");
            for (String line : track)
                System.out.println(line);
            throw e;
          }
        }
      } 
    } finally {
      removeDirectory(testDirectory);
    }
  }
  
  @Test
  public void newColumn() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
    Row[] rows2 = new Row[] { texasVuln, caliVuln };
    List<ColumnId> EXTRA_COLUMNS = new ArrayList<>(COLUMNS);
    ColumnId newColumn = new ColumnId("city", DataType.STRING.getCode());
    EXTRA_COLUMNS.add(newColumn);

    try {
      for (int i = 0; i < 2; i++) {
        if (i == 1)
          RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
        for (Compression compression : Compression.values()) {
          try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10)) {
            writer.begin();
            List<Entity> entities1 = new ArrayList<>();
            Entity entity1 = generateEntity("firstEntity", 1, rows2);
            entities1.add(entity1);
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities1);
  
            // Add a column called city to the mix.
            Row texasVulnExtra = new Row(1, 101l, "texas", "houston");
            Entity entity2 = Entity.buildEntity(ASSET_ID, "secondEntity", 1, TEST_UUID, EXTRA_COLUMNS, texasVulnExtra);
            List<Entity> entities2 = new ArrayList<>();
            entities2.add(entity2);
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities2);
            writer.commit();
  
            System.out.println(printTable(testDirectory));
            verifyTableReaderPOV(3, testDirectory, 2);
            verifyColumn(3, newColumn, testDirectory, 2);
          }
        }
      }
    } finally {
      removeDirectory(testDirectory);
    }

  }


  @Test
  public void completelyRandomWrites() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");

    FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
    try {
      for (int ii = 0; ii < 2; ii++) {
        if (ii == 1)
          RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
        for (Compression compression : Compression.values()) {
          int numTries = RANDOM.nextInt(50);
          try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10, null, null)) {
            for (int i = 0; i < numTries; i++) {
              writer.begin();
              int randomRows = RANDOM.nextInt(5000);
              Entity entity = randomEntity(1, randomRows);
              writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, Arrays.asList(entity));
              writer.commit();
            }
          }
        }
      }
    } finally {
      removeDirectory(testDirectory);
    }
  }

  @Test
  public void entitesRowContentsChange() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    Row[] rows2 = new Row[] {texasVuln, caliVuln};
    Row[] rowsNull3 = new Row[] {NULL_ROW, NULL_ROW, NULL_ROW};
    Row[] rows4 = new Row[] {texasVuln, caliVuln, zonaVuln, nyVuln};
    Row[] rows6 = new Row[] {texasVuln, caliVuln, zonaVuln, nyVuln, nevadaVuln, oregonVuln};
    FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
    int numEntities = 1000;
    try {
      for (int ii = 0; ii < 2; ii++) {
        if (ii == 1)
          RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
        for (Compression compression : Compression.values()) {
          try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10, null, null)) {
            writer.begin();
            List<Entity> entities4 = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
              Entity random4 = generateEntity(Integer.toString(i), 1, rows4);
              entities4.add(random4);
            }
  
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities4);
            writer.commit();
            verifyTableReaderPOV(numEntities*4, testDirectory, 10);
            int random = RANDOM.nextInt(999);
            verifyEntityReaderPOV(entities4.get(random), testDirectory);
  
            // Ok now every entity will see an increase of rows from 4 to 6
            List<Entity> entities6 = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
              Entity random6 = generateEntity(Integer.toString(i), 1, rows6);
              entities6.add(random6);
            }
  
            writer.begin();
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities6);
            writer.commit();
            verifyTableReaderPOV(numEntities*6, testDirectory, 10);
            random = RANDOM.nextInt(999);
            verifyEntityReaderPOV(entities6.get(random), testDirectory);
  
            // Ok now every entity will see an decrease of rows from 4 to 6
            List<Entity> entities2 = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
              Entity random2 = generateEntity(Integer.toString(i), 1, rows2);
              entities2.add(random2);
            }
  
            writer.begin();
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities2);
            writer.commit();
            verifyTableReaderPOV(numEntities*2, testDirectory, 10);
            random = RANDOM.nextInt(999);
            verifyEntityReaderPOV(entities2.get(random), testDirectory);
  
            // Ok now every entity will see a decrease to all null values
            List<Entity> entitiesNull3 = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
              Entity randomNull3 = generateEntity(Integer.toString(i), 1, rowsNull3);
              entitiesNull3.add(randomNull3);
            }
  
            writer.begin();
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entitiesNull3);
            writer.commit();
            verifyTableReaderPOV(numEntities*3, testDirectory, 10);
            random = RANDOM.nextInt(999);
            verifyEntityReaderPOV(entitiesNull3.get(random), testDirectory);
          }
  
        }
      }
    } finally {
      removeDirectory(testDirectory);
    }
  }

  @Test
  public void deleteOnly() throws IOException {
    for (int ii = 0; ii < 2; ii++) {
      if (ii == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        Path testDirectory = Files.createTempDirectory("filestore");
        FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
        try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10, null, null)) {
          writer.begin();
          for (int i = 0; i < 1000; i++) {
            Entity delete = new Entity(ASSET_ID, i, 100, null);
            writer.delete(TENANT, TABLE, INTERVAL, TIMESTAMP, delete);
          }
          writer.commit();
          verifyTableReaderPOV(0, testDirectory, 0);
        } finally {
          removeDirectory(testDirectory);
        }
      }
    }
  }


  @Test
  public void verifySameXactError() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
    for (int i = 0; i < 2; i++) {
      if (i == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        Assertions.assertThrows(XactError.class, () -> {
          try (ArmorWriter writer = new ArmorWriter("aw1", store, compression, 10, null, null)) {
            List<Entity> entities = new ArrayList<>();
            Entity random = generateEntity("same", 1, null);
            entities.add(random);
            writer.begin();
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
            writer.commit();
            writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
            writer.commit();
          } finally {
            removeDirectory(testDirectory);
          }
        });
      }
    }
  }
  
  @Test
  public void switchCompression() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    // Ensure we can switch between compression methods if we need to.
    int numEntities = 10;
    Row[] rows = new Row[] {texasVuln, caliVuln, zonaVuln, nyVuln};
    Row[] rows2 = new Row[] {texasVuln, caliVuln, zonaVuln, nyVuln, nevadaVuln};

    try  {
      // First create armor files with zstd
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.ZSTD, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          Entity random = generateEntity(i, 1, rows);
          entities.add(random);
        }
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(numEntities*4, testDirectory, 10);
        int random = RANDOM.nextInt(10);
        verifyEntityReaderPOV(entities.get(random), testDirectory);
      }
      
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          Entity random = generateEntity(i, 2, null);
          entities.add(random);
        }
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(numEntities, testDirectory, 10);
        int random = RANDOM.nextInt(10);
        verifyEntityReaderPOV(entities.get(random), testDirectory);
      }
      
      
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.ZSTD, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          Entity random = generateEntity(i, 3, rows);
          entities.add(random);
        }
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(numEntities*4, testDirectory, 10);
        int random = RANDOM.nextInt(10);
        verifyEntityReaderPOV(entities.get(random), testDirectory);
      }
      
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          Entity random = generateEntity(i, 4, rows2);
          entities.add(random);
        }
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(numEntities*5, testDirectory, 10);
        int random = RANDOM.nextInt(10);
        verifyEntityReaderPOV(entities.get(random), testDirectory);
      }
    } finally {
      removeDirectory(testDirectory);
    }
  }
  
  
  @Test
  public void mixCompression() throws IOException {
    // Ensure shards with different compression methods can still be read
    // with no problem.
    Path testDirectory = Files.createTempDirectory("filestore");
    try {
      ModShardStrategy shardStrategy = new ModShardStrategy(10);
      FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));
      // Shard 1
      Entity entity1 =  generateEntity(1, 1, null);
      int entity1Shard = shardStrategy.shardNum(1);
      ColumnId testColumn = new ColumnId("vuln", DataType.STRING.getCode());
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        entities.add(entity1);
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(1, testDirectory, 1);
        verifyEntityReaderPOV(entity1, testDirectory);
        ColumnMetadata md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
  
      }
      
      Entity entity2 =  generateEntity(2, 2, null);
      int entity2Shard = shardStrategy.shardNum(2);
  
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.ZSTD, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        entities.add(entity2);
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(2, testDirectory, 2);
        verifyEntityReaderPOV(entity1, testDirectory);
        verifyEntityReaderPOV(entity2, testDirectory);
        
        ColumnMetadata md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity2Shard), testColumn));
        assertEquals(Compression.ZSTD.toString(), md.getCompressionAlgorithm());
      }
      
      Entity entity3 =  generateEntity(3, 3, null);
      int entity3Shard = shardStrategy.shardNum(3);
  
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        entities.add(entity3);
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(3, testDirectory, 3);
        verifyEntityReaderPOV(entity1, testDirectory);
        verifyEntityReaderPOV(entity2, testDirectory);
        verifyEntityReaderPOV(entity3, testDirectory);
        
        ColumnMetadata md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity2Shard), testColumn));
        assertEquals(Compression.ZSTD.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity3Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
      }
      
      Entity entity4 =  generateEntity(4, 4, null);
      int entity4Shard = shardStrategy.shardNum(4);
  
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.ZSTD, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        entities.add(entity4);
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(4, testDirectory, 4);
        verifyEntityReaderPOV(entity1, testDirectory);
        verifyEntityReaderPOV(entity2, testDirectory);
        verifyEntityReaderPOV(entity3, testDirectory);
        verifyEntityReaderPOV(entity4, testDirectory);
        
        ColumnMetadata md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity2Shard), testColumn));
        assertEquals(Compression.ZSTD.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity3Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity4Shard), testColumn));
        assertEquals(Compression.ZSTD.toString(), md.getCompressionAlgorithm());
      }
      
      // Final one change them all to no compression.
      entity1 =  generateEntity(1, 5, null);
      entity2 =  generateEntity(2, 5, null);
      entity3 =  generateEntity(3, 5, null);
      entity4 =  generateEntity(4, 5, null);
      Entity entity5 =  generateEntity(5, 5, null);
      int entity5Shard = shardStrategy.shardNum(5);
      
      try (ArmorWriter writer = new ArmorWriter("aw1", store, Compression.NONE, 10, null, null)) {
        writer.begin();
        List<Entity> entities = new ArrayList<>();
        entities.add(entity1);
        entities.add(entity2);
        entities.add(entity3);
        entities.add(entity4);
        entities.add(entity5);
        writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
        writer.commit();
        
        verifyTableReaderPOV(5, testDirectory, 5);
        verifyEntityReaderPOV(entity1, testDirectory);
        verifyEntityReaderPOV(entity2, testDirectory);
        verifyEntityReaderPOV(entity3, testDirectory);
        verifyEntityReaderPOV(entity4, testDirectory);
        verifyEntityReaderPOV(entity5, testDirectory);
  
        ColumnMetadata md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity1Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity2Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity3Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity4Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
        
        md = store.getColumnMetadata(TENANT, TABLE, new ColumnShardId(new ShardId(TENANT, TABLE, INTERVAL.getInterval(), INTERVAL.getIntervalStart(TIMESTAMP), entity5Shard), testColumn));
        assertEquals(Compression.NONE.toString(), md.getCompressionAlgorithm());
      }
    } finally {
      removeDirectory(testDirectory);
    }
  }

  @Test
  public void burstCheck() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    int numShards = 10;
    int numEntities = 1000;
    Row[] rows = new Row[] {texasVuln, caliVuln};
    for (int ii = 0; ii < 2; ii++) {
      if (ii == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        try {
          // Test with 10 shards
          FileWriteStore store = new FileWriteStore(testDirectory, new ModShardStrategy(10));

          ArmorWriter writer = new ArmorWriter("aw1", store, compression, numShards, null, null);
          writer.begin();
          List<Entity> entities = new ArrayList<>();
          for (int i = 0; i < 1000; i++) {
            Entity random = generateEntity(i, 1, rows);
            entities.add(random);
          }

          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
          writer.commit();
          writer.close();

          verifyTableReaderPOV(numEntities*2, testDirectory, numShards);
          int random = RANDOM.nextInt(999);
          verifyEntityReaderPOV(entities.get(random), testDirectory);

          // Now lets delete them all
          writer = new ArmorWriter("aw1", store, Compression.ZSTD, numShards, null, null);
          writer.begin();
          for (int i = 0; i < 1000; i++) {
            Entity delete = new Entity(ASSET_ID, i, Integer.MAX_VALUE, null);
            writer.delete(TENANT, TABLE, INTERVAL, TIMESTAMP, delete);
          }
          writer.commit();
          writer.close();

          verifyTableReaderPOV(0, testDirectory, numShards);
          verifyEntityDeletedReaderPOV(entities.get(random), testDirectory);

          // Add it back
          writer = new ArmorWriter("aw1", store, compression, numShards, null, null);
          writer.begin(); 
          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities);
          writer.commit();

          verifyTableReaderPOV(numEntities*2, testDirectory, numShards);
          random = RANDOM.nextInt(999);
          verifyEntityReaderPOV(entities.get(random), testDirectory);

          // NOTE: Notice we didn't close the writer yet! Add another 1K.
          List<Entity> entities1 = new ArrayList<>();
          for (int i = 1000; i < 2000; i++) {
            Entity random1 = generateEntity(i, 1, rows);
            entities1.add(random1);
          }

          writer.begin();
          // Attempt to also try and double count it shouldn't double count.
          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities1);
          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities1);

          writer.commit();

          verifyTableReaderPOV((2*numEntities)*2, testDirectory, numShards);
          random = RANDOM.nextInt(999);
          verifyEntityReaderPOV(entities.get(random), testDirectory);
          verifyEntityReaderPOV(entities1.get(random), testDirectory);
          writer.close();

          // Finally lets add more entites to the table in 2 valid batch before we save and finish this test.
          writer = new ArmorWriter("aw1", store, compression, numShards, null, null);
          writer.begin();      
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
          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities3);
          writer.write(TENANT, TABLE, INTERVAL, TIMESTAMP, entities2);
          writer.commit();
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
  }
}
