package com.rapid7.armor;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.fast.FastArmorBlock;
import com.rapid7.armor.read.fast.FastArmorBlockReader;
import com.rapid7.armor.read.fast.FastArmorReader;
import com.rapid7.armor.read.slow.SlowArmorReader;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.FileReadStore;
import com.rapid7.armor.store.FileWriteStore;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.writers.ArmorWriter;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static com.rapid7.armor.interval.Interval.SINGLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import tech.tablesaw.columns.Column;
import org.junit.jupiter.api.Test;

public class FileWriteStoreTest {

  private void checkEntityIndexRecord(EntityRecord eir, int rowGroupOffset, int valueLength, int nullLength, byte deleted) {
    assertEquals(valueLength, eir.getValueLength());
    assertEquals(rowGroupOffset, eir.getRowGroupOffset());
    assertEquals(nullLength, eir.getNullLength());
    assertEquals(deleted, eir.getDeleted());
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

  @Test
  public void emptyWithSome() throws IOException {
    String instanceId = UUID.randomUUID().toString();
    Path testDirectory = Files.createTempDirectory("filestore");
    FileWriteStore fileStore = new FileWriteStore(testDirectory, new ModShardStrategy(1));
    ColumnId vuln = new ColumnId("vuln", DataType.INTEGER.getCode());
    Entity e1 = Entity.buildEntity("asset", 1, 1, instanceId, vuln);

    Entity e2 = Entity.buildEntity("asset", 2, 1, instanceId, vuln);

    Entity e3 = Entity.buildEntity("asset", 3, 1, instanceId, vuln);
    e3.addRow(1);

    Entity e4 = Entity.buildEntity("asset", 4, 1, instanceId, vuln);

    Entity e5 = Entity.buildEntity("asset", 5, 1, instanceId, vuln);

    Entity e6 = Entity.buildEntity("asset", 6, 1, instanceId, vuln);

    Entity e7 = Entity.buildEntity("asset", 7, 1, instanceId, vuln);

    for (int i = 0; i < 2; i++) {
      if (i == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        try {
          try (ArmorWriter armorWriter = new ArmorWriter("test", fileStore, compression, 10, null, null)) {
            armorWriter.begin();
            armorWriter.write("myorg", "testtable", SINGLE, Instant.now(), Arrays.asList(e1, e2, e3, e4, e5, e6, e7));
            armorWriter.columnEntityRecords("myorg", "testtable", SINGLE, Instant.now(), "vuln", 0);
            armorWriter.commit();
          }

          try (ArmorWriter armorWriter2 = new ArmorWriter("test", fileStore, compression, 10, null, null)) {
            armorWriter2.begin();
            Entity e8 = Entity.buildEntity("asset", 8, 1, null, vuln);
            armorWriter2.write("myorg", "testtable", SINGLE, Instant.now(), Collections.singletonList(e8));
            armorWriter2.commit();
          }
        } finally {
          removeDirectory(testDirectory);
        }
      }
    }
  }

  @Test
  public void basicTests() throws Exception {
    Path testDirectory = Files.createTempDirectory("filestore");
    FileWriteStore fileStore = new FileWriteStore(testDirectory, new ModShardStrategy(1));
    FileReadStore fileReadStore = new FileReadStore(testDirectory);
    String myorg = "myorg";
    String table = "vulntable";
    ColumnId name = new ColumnId("name", DataType.STRING.getCode());
    ColumnId time = new ColumnId("time", DataType.LONG.getCode());
    ColumnId vuln = new ColumnId("vuln", DataType.INTEGER.getCode());
    ColumnId asset = new ColumnId("assetId", DataType.INTEGER.getCode());
    List<ColumnId> columns = Arrays.asList(name, time, vuln);
    String instanceId = UUID.randomUUID().toString();
    for (int i = 0; i < 2; i++) {
      if (i == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);

      for (Compression compression : Compression.values()) {
        try (ArmorWriter armorWriter = new ArmorWriter("test", fileStore, compression, 10, () -> 1, null)) {
          Entity e11 = Entity.buildEntity("assetId", 1, 1, instanceId, name, time, vuln);
          e11.addRows(
              "a", 6L, 1,
              "b", 5L, 2,
              "c", 4L, 3,
              "d", 3L, 4,
              "e", 2L, 5,
              "e", 2L, 5,
              "f", 1L, 6
              );

          Entity e12 = Entity.buildEntity("assetId", 1, 2, instanceId, name, time, vuln);  // Should be this one
          e12.addRows(
              "a", 7L, 1,
              "b", 8L, 2,
              "c", null, 3,
              "d", 9L, 4,
              "e", 10L, 5,
              "e", 11L, 6
              );

          Entity e10 = Entity.buildEntity("assetId", 1, 0, instanceId, name, time, vuln);
          e10.addRows(
              "a", 6L, null,
              "a", 5L, null,
              "a", null, null,
              "a", 3L, null,
              "a", 2L, 5,
              "a", 1L, 6);

          Entity e20 = Entity.buildEntity("assetId", 2, 0, instanceId, time, vuln);
          e20.addRows(
              6L, null,
              5L, null,
              null, null,
              3L, null,
              2L, 5,
              null, 6);
          armorWriter.begin();
          armorWriter.write(myorg, table, SINGLE, Instant.now(), Arrays.asList(e11, e12, e10, e20));
          armorWriter.commit();
          armorWriter.begin();
          // Verify store/shard stuff
          List<ShardId> shardIds = fileStore.findShardIds(myorg, table, SINGLE, Instant.now(), "vuln");
          assertFalse(shardIds.isEmpty());
          ShardId shardId = shardIds.get(0);
          assertEquals(Sets.newHashSet(name, asset, vuln, time), Sets.newHashSet(fileStore.getColumnIds(shardId)));

          // 12 rows, 2 entities 1 and 2, freebytes 0
          Map<Integer, EntityRecord> vulnEntityRecords1 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata cmd1 = armorWriter.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(2, vulnEntityRecords1.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(cmd1.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd1.getMaxValue());
          assertEquals(Double.valueOf(1.0), cmd1.getMinValue());
          assertEquals(12, cmd1.getNumRows());
          assertEquals(2, cmd1.getNumEntities());

          checkEntityIndexRecord(vulnEntityRecords1.get(1), 0, 24, 0, (byte) 0);
          checkEntityIndexRecord(vulnEntityRecords1.get(2), 24, 24, 15, (byte) 0);

          // Delete the entity 1
          Entity delete1 = new Entity("assetId", 1, 1000l, "test");
          armorWriter.delete(myorg, table, SINGLE, Instant.now(), delete1);

          armorWriter.commit();
          armorWriter.begin();

          Map<Integer, EntityRecord> vulnEntityRecords2 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata cmd2 = armorWriter.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(1, vulnEntityRecords2.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(cmd2.getFragmentationLevel()));
          //assertEquals(Integer.valueOf(50), Integer.valueOf(cmd2.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd2.getMaxValue());
          assertEquals(Double.valueOf(5.0), cmd2.getMinValue());
          assertEquals(6, cmd2.getNumRows());
          assertEquals(1, cmd2.getNumEntities());
          //checkEntityIndexRecord(vulnEntityRecords2.get(1), 0, 24, 0, (byte) 1);
          // since we compact automatically, the only record is for entityId 2
          checkEntityIndexRecord(vulnEntityRecords2.get(2), 0, 24, 15, (byte) 0);


          // Write a new entry same exact thing
          Entity e21 = Entity.buildEntity("assetId", 2, 1, instanceId, columns);
          e21.addRows(
              "1", 6L, null,
              "1", 5L, null,
              "1", null, null,
              "1", 3L, null,
              "1", 2L, 5,
              "1", null, 6);

          armorWriter.write(myorg, table, SINGLE, Instant.now(), Collections.singletonList(e21));
          armorWriter.commit();
          armorWriter.begin();

          Map<Integer, EntityRecord> vulnEntityRecords3 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata cmd3 = armorWriter.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(1, vulnEntityRecords3.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(cmd3.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd3.getMaxValue());
          assertEquals(Double.valueOf(5.0), cmd3.getMinValue());
          assertEquals(6, cmd3.getNumRows());
          assertEquals(1, cmd3.getNumEntities());
          checkEntityIndexRecord(vulnEntityRecords3.get(2), 0, 24, 15, (byte) 0);

          // Write a new entity as well as less rows in version 2
          Entity e23 = Entity.buildEntity("assetId", 2, 3, instanceId, columns);
          e23.addRow("1", 6L, null);
          e23.addRow("1", 5L, null);
          e23.addRow("1", null, null);
          e23.addRow("1", 3L, null);
          e23.addRow(null, null, 6);

          Entity e31 = Entity.buildEntity("assetId", 3, 1, instanceId, columns);
          e31.addRow("1", null, 2);
          e31.addRow("1", null, -1);

          armorWriter.write(myorg, table, SINGLE, Instant.now(), Arrays.asList(e23, e31));
          armorWriter.commit();
          armorWriter.begin();

          Map<Integer, EntityRecord> records4 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata md4 = armorWriter.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(2, records4.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(md4.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), md4.getMaxValue());
          assertEquals(Double.valueOf(-1.0), md4.getMinValue());
          assertEquals(7, md4.getNumRows());
          assertEquals(2, md4.getNumEntities());
          checkEntityIndexRecord(records4.get(2), 0, 20, 15, (byte) 0);
          checkEntityIndexRecord(records4.get(3), 35, 8, 0, (byte) 0);

          // Overwrite existing one but this time expand the row count
          Entity e32 = Entity.buildEntity("assetId", 3, 2, instanceId, columns);
          e32.addRow("1", null, 6);
          e32.addRow("1", null, -1);
          e32.addRow(null, null, null);

          ArmorWriter amrorWriter2 = new ArmorWriter("test", fileStore, compression, 10, null, null);
          amrorWriter2.begin();
          amrorWriter2.write(myorg, table, SINGLE, Instant.now(), Collections.singletonList(e32));
          amrorWriter2.commit();
          amrorWriter2.begin();

          Map<Integer, EntityRecord> records5 = amrorWriter2.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata md5 = amrorWriter2.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(2, records5.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(md5.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), md5.getMaxValue());
          assertEquals(Double.valueOf(-1.0), md5.getMinValue());
          assertEquals(8, md5.getNumRows());
          assertEquals(2, md5.getNumEntities());
          checkEntityIndexRecord(records5.get(2), 0, 20, 15, (byte) 0);
          checkEntityIndexRecord(records5.get(3), 35, 12, 18, (byte) 0);

          amrorWriter2.close(); // Close this FS and open a new one to test the load.

          SlowArmorReader armorReader = new SlowArmorReader(fileReadStore);
          ShardId zeroShardId = ShardId.buildShardId(myorg, table, SINGLE, Instant.now(), 0);
          ColumnMetadata aShard = armorReader.getColumnMetadata(zeroShardId, "vuln");
          assertEquals(DataType.INTEGER, aShard.getColumnType());
          assertEquals(Integer.valueOf(0), Integer.valueOf(aShard.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), aShard.getMaxValue());
          assertEquals(Double.valueOf(-1.0), aShard.getMinValue());
          assertEquals(8, aShard.getNumRows());
          assertEquals(2, aShard.getNumEntities());

          armorReader.getColumn(myorg, table, SINGLE, Instant.now(), "assetid");

          Column<?> vulnInts = armorReader.getColumn(myorg, table, SINGLE, Instant.now(), "vuln");
          assertArrayEquals(
              new Integer[] {null, null, null, null, 6, 6, -1, null},
              vulnInts.asObjectArray());

          FastArmorReader fastArmorReader = new FastArmorReader(fileReadStore);
          ColumnMetadata rShard = armorReader.getColumnMetadata(zeroShardId, "vuln");
          assertEquals(DataType.INTEGER, rShard.getColumnType());
          assertEquals(Integer.valueOf(0), Integer.valueOf(rShard.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), rShard.getMaxValue());
          assertEquals(Double.valueOf(-1.0), rShard.getMinValue());
          assertEquals(8, rShard.getNumRows());
          assertEquals(2, rShard.getNumEntities());

          // Asset 2
          //"1", 6l, null
          //"1", 5l, null
          //"1", null, null
          //"1", 3l, null
          // null, null, 6

          // Asset 3
          //"1", null, 6
          //"1", null, -1
          //null, null, null
          FastArmorBlockReader fastReader1 = fastArmorReader.getColumn(zeroShardId, "vuln");
          FastArmorBlock a1a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a2a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a3a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a4a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a5a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a6a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a7a = fastReader1.getIntegerBlock(1);
          assertTrue(fastReader1.hasNext());
          FastArmorBlock a8a = fastReader1.getIntegerBlock(1);
          assertFalse(fastReader1.hasNext());

          FastArmorBlockReader fastReader1a = fastArmorReader.getColumn(zeroShardId, "vuln");
          FastArmorBlock a1aa = fastReader1a.getIntegerBlock(2);
          assertTrue(fastReader1a.hasNext());
          FastArmorBlock a2aa = fastReader1a.getIntegerBlock(10);
          assertFalse(fastReader1a.hasNext());

          FastArmorBlockReader fastReader2 = fastArmorReader.getColumn(zeroShardId, "name");
          FastArmorBlock a1b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a2b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a3b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a4b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a5b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a6b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a7b = fastReader2.getStringBlock(1);
          assertTrue(fastReader2.hasNext());
          FastArmorBlock a8b = fastReader2.getStringBlock(1);
          assertFalse(fastReader2.hasNext());

          FastArmorBlockReader fastReader2aa = fastArmorReader.getColumn(zeroShardId, "name");
          FastArmorBlock a1ba = fastReader2aa.getStringBlock(2);
          assertTrue(fastReader2aa.hasNext());
          FastArmorBlock a2ba = fastReader2aa.getStringBlock(10);
          assertFalse(fastReader2aa.hasNext());

        } finally {
          removeDirectory(testDirectory);
        }
      }
    }
  }
}
