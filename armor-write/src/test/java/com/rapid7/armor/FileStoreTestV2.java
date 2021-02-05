package com.rapid7.armor;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.entity.Row;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.read.FastArmorBlock;
import com.rapid7.armor.read.SlowArmorReader;
import com.rapid7.armor.read.FastArmorColumnReader;
import com.rapid7.armor.read.FastArmorReader;
import com.rapid7.armor.read.FastArmorShard;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import tech.tablesaw.columns.Column;
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
  
  
//  private void verifyEntityReaderPOV(Entity entity, Path path) {
//    FileReadStore readStore = new FileReadStore(path);
//    Entity testEntity = Entity.buildEntity(entity.getEntityIdColumn(), entity, entity.getVersion(), TEST_UUID);
//    for (ShardId shardId : readStore.findShardIds(TENANT, TABLE)) {
//      for (ColumnName columnName : COLUMNS) {
//        
//        // FAS doesn't load the entity dictionary if it existed, so we need to load Column
//        FastArmorShard fas = readStore.getFastArmorShard(shardId, columnName.getName());
//        fas.getValuesForRecord(testEntity.getEntityId());
//        
//        
//      }
//    }
//    
//    
//    int totalRows = 0;
//    for (ShardId shardId : shardIds) {
//      Integer shardRows = null;
//      for (ColumnName columnName : COLUMNS) {
//        FastArmorShard fas = readStore.getFastArmorShard(shardId, columnName.getName());
//        FastArmorColumnReader far = fas.getFastArmorColumnReader();
//        DataType dt = fas.getDataType();
//        FastArmorBlock fab = null;
//        switch (dt) {
//        case INTEGER:
//          fab = far.getIntegerBlock(5000);
//          break;
//        case LONG:
//          fab = far.getLongBlock(5000);
//          break;
//        case STRING:
//          fab = far.getStringBlock(5000);
//          break;
//        }
//        
//        if (shardRows == null) {
//          shardRows = fab.getNumRows();
//        } else if (shardRows != fab.getNumRows()) {
//          throw new RuntimeException("Within a shard the two column row counts do not match");
//        }
//      }
//      totalRows += shardRows;
//    }
//    assertEquals(expectedNumberRows, totalRows);
//    
//    // Check schema against store
//    
//    //
//  }
  
  private void verifyTableReaderPOV(int expectedNumberRows, Path path, int numShards) {
    FileReadStore readStore = new FileReadStore(path);
    List<ShardId> shardIds = readStore.findShardIds(TENANT, TABLE);
    assertEquals(numShards, shardIds.size());
    int totalRows = 0;
    for (ShardId shardId : shardIds) {
      Integer shardRows = null;
      for (ColumnName columnName : COLUMNS) {
        FastArmorShard fas = readStore.getFastArmorShard(shardId, columnName.getName());
        FastArmorColumnReader far = fas.getFastArmorColumnReader();
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
      //verifyEntityReaderPov(entities.get(random));
      
      
      
    } finally {
      removeDirectory(testDirectory);
    }
  }
}
