package com.rapid7.armor;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.io.PathBuilder;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.S3ReadStore;
import com.rapid7.armor.store.S3WriteStore;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.writers.ArmorWriter;
import com.rapid7.armor.xact.DistXactRecord;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.rapid7.armor.interval.Interval.SINGLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.findify.s3mock.S3Mock;

public class S3StoreTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(S3WriteStore.class);
  private static final String TEST_BUCKET = "testbucket";
  private static final S3Mock S3_MOCK = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
  private static AmazonS3 client;

  @BeforeAll
  public static void setupBucket() throws ParseException {
    S3_MOCK.start();
    EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
    client = AmazonS3ClientBuilder
        .standard()
        .withPathStyleAccessEnabled(true)
        .withEndpointConfiguration(endpoint)
        .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
        .build();
    client.createBucket(TEST_BUCKET);
  }
  
  @BeforeEach
  public void clearBucket() {
    ObjectListing objectListing = client.listObjects(TEST_BUCKET);
    while (true) {
      for (S3ObjectSummary s3ObjectSummary : objectListing.getObjectSummaries()) {
        client.deleteObject(TEST_BUCKET, s3ObjectSummary.getKey());
      }
      if (objectListing.isTruncated()) {
        objectListing = client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
  }

  @AfterAll
  public static void shutdown() {
    LOGGER.info("Tests are all done, shutting down s3 mocker");
    client.shutdown();
    S3_MOCK.shutdown();
    LOGGER.info("S3 mock is shutdown");
  }

  private void checkEntityIndexRecord(EntityRecord eir, int rowGroupOffset, int valueLength, int nullLength, byte deleted) {
    assertEquals(valueLength, eir.getValueLength());
    assertEquals(rowGroupOffset, eir.getRowGroupOffset());
    assertEquals(nullLength, eir.getNullLength());
    assertEquals(deleted, eir.getDeleted());
  }
  
  @Test
  public void deleteTenant() throws AmazonServiceException, SdkClientException, JsonProcessingException {
    String current1 = UUID.randomUUID().toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> currentValue1 = new HashMap<>();
    currentValue1.put("current", current1);
    
    // TODO: Setup test for metadata
    //client.putObject(TEST_BUCKET, "org10/table1/table-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
    
    // TODO: Setup test for metadata
    //client.putObject(TEST_BUCKET, "org10/table2/table-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table2/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table2/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table2/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org10/table2/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
    
    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
    List<ShardId> shards = writeStore.findShardIds("org10", "table1", SINGLE, Instant.now());
    assertEquals(1, shards.size());
    shards = writeStore.findShardIds("org10", "table2", SINGLE, Instant.now());
    assertEquals(1, shards.size());

    writeStore.deleteTable("org10", "table1");
    shards = writeStore.findShardIds("org10", "table1", SINGLE, Instant.now());
    assertEquals(0, shards.size());
    
    shards = writeStore.findShardIds("org10", "table2", SINGLE, Instant.now());
    assertEquals(1, shards.size());
  }

  @Test
  public void writeSchemaShardDiscovery() throws SdkClientException, JsonProcessingException {
    // First lets setup potential shards and directories in s3, directly write to s3
    String current1 = UUID.randomUUID().toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> currentValue1 = new HashMap<>();
    currentValue1.put("current", current1);

    String current2 = UUID.randomUUID().toString();
    HashMap<String, String> currentValue2 = new HashMap<>();
    currentValue2.put("current", current2);

    client.putObject(TEST_BUCKET, "org1/table1/table-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));

    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current2 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current2 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue2));

    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(2));
    ShardId shard0 = writeStore.findShardId("org1", "table1", SINGLE, Instant.now(), 0);
    assertEquals(0, shard0.getShardNum());
    assertEquals("org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0", shard0.shardIdPath());
    ShardId shard1 = writeStore.findShardId("org1", "table1", SINGLE, Instant.now(), 1);
    assertEquals(1, shard1.getShardNum());
    assertEquals("org1/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1", shard1.shardIdPath());


    List<ShardId> shardIds = writeStore.findShardIds("org1", "table1", SINGLE, Instant.now());
    assertTrue(shardIds.contains(shard0));
    assertTrue(shardIds.contains(shard1));

    List<ShardId> nameShardIds = writeStore.findShardIds("org1", "table1", SINGLE, Instant.now(), "name");
    assertTrue(nameShardIds.contains(shard0));
    assertTrue(nameShardIds.contains(shard1));

    List<ShardId> levelShardIds = writeStore.findShardIds("org1", "table1", SINGLE, Instant.now(), "level");
    assertTrue(levelShardIds.contains(shard0));
    assertTrue(levelShardIds.contains(shard1));

    ColumnId nameColumn = new ColumnId("name", "S");
    ColumnId levelColumn = new ColumnId("level", "I");

    List<ColumnId> shard0columnIds = writeStore.getColumnIds(shard0);
    assertTrue(shard0columnIds.contains(nameColumn));
    assertTrue(shard0columnIds.contains(levelColumn));

    List<ColumnId> shard1columnIds = writeStore.getColumnIds(shard1);
    assertTrue(shard1columnIds.contains(nameColumn));
    assertTrue(shard1columnIds.contains(levelColumn));

    assertEquals(shard0, ShardId.buildShardId("org1", "table1", SINGLE, Instant.now(), 0));
    assertEquals(shard1, ShardId.buildShardId("org1", "table1", SINGLE, Instant.now(), 1));

    S3ReadStore readStore = new S3ReadStore(client, TEST_BUCKET);
    ShardId zeroShard0Table1 = ShardId.buildShardId("org1", "table1", SINGLE, Instant.now(), 0);
    assertTrue(readStore.shardIdExists(zeroShard0Table1));
    ShardId zeroShard1Table1 = ShardId.buildShardId("org1", "table1", SINGLE, Instant.now(), 1);
    assertTrue(readStore.shardIdExists(zeroShard1Table1));
    assertFalse(readStore.shardIdExists(ShardId.buildShardId("org1", "table1", SINGLE, Instant.now(), 100)));

    List<ShardId> readShardIds = readStore.findShardIds("org1", "table1", SINGLE, Instant.now());
    assertTrue(readShardIds.contains(shard0));
    assertTrue(readShardIds.contains(shard1));

    List<ShardId> readNameShardIds = readStore.findShardIds("org1", "table1", SINGLE, Instant.now(), "name");
    assertTrue(readNameShardIds.contains(shard0));
    assertTrue(readNameShardIds.contains(shard1));

    List<ShardId> levelNameShardIds = readStore.findShardIds("org1", "table1", SINGLE, Instant.now(), "level");
    assertTrue(levelNameShardIds.contains(shard0));
    assertTrue(levelNameShardIds.contains(shard1));

    List<ColumnId> shard0Columns = readStore.getColumnIds(shard0);
    assertTrue(shard0Columns.contains(nameColumn));
    assertTrue(shard0Columns.contains(levelColumn));

    List<ColumnId> shard1Columns = readStore.getColumnIds(shard1);
    assertTrue(shard1Columns.contains(nameColumn));
    assertTrue(shard1Columns.contains(levelColumn));

    List<ColumnId> tableColumns = readStore.getColumnIds("org1", "table1", SINGLE, Instant.now());
    assertTrue(tableColumns.contains(nameColumn));
    assertTrue(tableColumns.contains(levelColumn));

    List<String> tables = readStore.getTables("org1");
    assertTrue(tables.contains("table1"));
  }
  
  @Test
  public void verifyCopyShard() throws AmazonServiceException, SdkClientException, IOException {
    String current1 = UUID.randomUUID().toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> currentValue1 = new HashMap<>();
    currentValue1.put("current", current1);
    
    client.putObject(TEST_BUCKET, "orgA/table1/table-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
    
    // Make these distinct in name to ensure test is valid.
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current1 + "/name_S_a", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current1 + "/level_I_a", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + current1 + "/shard-metadata.armora", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
 
    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
    try (ArmorWriter aw = new ArmorWriter("test", writeStore, Compression.NONE, 1)) {
      Instant now = Instant.now();
      String interavlStart = Interval.WEEKLY.getIntervalStart(now);
      aw.snapshotCurrentToInterval("orgA", "table1", Interval.WEEKLY, now);
      
      String expectedCurrent0 = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + interavlStart + "/0/" + DistXactRecord.CURRENT_MARKER;
      String expectedCurrent1 = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + interavlStart + "/1/" + DistXactRecord.CURRENT_MARKER;
      assertTrue(client.doesObjectExist(TEST_BUCKET, expectedCurrent0));
      assertTrue(client.doesObjectExist(TEST_BUCKET, expectedCurrent1));
      
      String expectedNameS = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + interavlStart + "/0/" + current1 + "/name_S";
      String expectedNameSa = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + interavlStart + "/1/" + current1 + "/name_S_a";
      assertTrue(client.doesObjectExist(TEST_BUCKET, expectedNameS));
      assertTrue(client.doesObjectExist(TEST_BUCKET, expectedNameSa));
    }
  }
  
  @Test
  public void verifyCopyShardRetry() throws AmazonServiceException, SdkClientException, IOException {
    String currentId = UUID.randomUUID().toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> currentData = new HashMap<>();
    
    String wrongCurrentId = UUID.randomUUID().toString();
    currentData.put("current", wrongCurrentId);
    
    client.putObject(TEST_BUCKET, "orgA/table1/table-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + currentId + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + currentId + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + currentId + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/0/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentData));
    
    // Make these distinct in name to ensure test is valid.
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + currentId + "/name_S_a", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + currentId + "/level_I_a", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + currentId + "/shard-metadata.armora", " Empty content");
    client.putObject(TEST_BUCKET, "orgA/table1/" + SINGLE.getInterval() + Constants.STORE_DELIMETER + Instant.ofEpochMilli(0) + "/1/" + DistXactRecord.CURRENT_MARKER, mapper.writeValueAsString(currentData));
    
    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
    try (ArmorWriter aw = new ArmorWriter("test", writeStore, Compression.NONE, 1)) {
      Instant now = Instant.now();
      RuntimeException runtimeException = assertThrows(RuntimeException.class, () -> aw.snapshotCurrentToInterval("orgA", "table1", Interval.WEEKLY, now));
      assertTrue(runtimeException.getMessage().contains("Expected current shard to contain objects"));
      
      String intervalStart = Interval.WEEKLY.getIntervalStart(now);
      String copiedWeeklyShard0 = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + intervalStart + "/0/" + DistXactRecord.CURRENT_MARKER;
      String copiedWeeklyShard1 = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + intervalStart + "/1/" + DistXactRecord.CURRENT_MARKER;
      assertFalse(client.doesObjectExist(TEST_BUCKET, copiedWeeklyShard0));
      assertFalse(client.doesObjectExist(TEST_BUCKET, copiedWeeklyShard1));
  
      String wrongCopiedWeeklyShard0Data = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + intervalStart + "/0/" + wrongCurrentId + "/name_S";
      String wrongCopiedWeeklyShard1Data = "orgA/table1/" + Interval.WEEKLY.getInterval() + Constants.STORE_DELIMETER + intervalStart + "/1/" + wrongCurrentId + "/name_S_a";
      assertFalse(client.doesObjectExist(TEST_BUCKET, wrongCopiedWeeklyShard0Data));
      assertFalse(client.doesObjectExist(TEST_BUCKET, wrongCopiedWeeklyShard1Data));
    }
  }

  @Test
  public void basicTests() throws Exception {
    String myorg = "myorg1";
    String table = "vulntable2";
    ColumnId name = new ColumnId("name", DataType.STRING.getCode());
    ColumnId time = new ColumnId("time", DataType.LONG.getCode());
    ColumnId vuln = new ColumnId("vuln", DataType.INTEGER.getCode());
    ColumnId asset = new ColumnId("assetId", DataType.INTEGER.getCode());
    List<ColumnId> columns = Arrays.asList(name, time, vuln);
    for (int i = 0; i < 2; i++) {
      if (i == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
        try (ArmorWriter armorWriter = new ArmorWriter("name", writeStore, compression, 10, () -> 1, null)) {
          armorWriter.begin();
          Entity e11 = Entity.buildEntity("assetId", 1, 1, null, name, time, vuln);
          e11.addRows(
              "a", 6L, 1,
              "b", 5L, 2,
              "c", 4L, 3,
              "d", 3L, 4,
              "e", 2L, 5,
              "e", 2L, 5,
              "f", 1L, 6
              );

          Entity e12 = Entity.buildEntity("assetId", 1, 2, null, name, time, vuln);  // Should be this one
          e12.addRows(
              "a", 7L, 1,
              "b", 8L, 2,
              "c", null, 3,
              "d", 9L, 4,
              "e", 10L, 5,
              "e", 11L, 6
              );

          Entity e10 = Entity.buildEntity("assetId", 1, 0, null, name, time, vuln);
          e10.addRows(
              "a", 6L, null,
              "a", 5L, null,
              "a", null, null,
              "a", 3L, null,
              "a", 2L, 5,
              "a", 1L, 6);

          Entity e20 = Entity.buildEntity("assetId", 2, 0, null, time, vuln);
          e20.addRows(
              6L, null,
              5L, null,
              null, null,
              3L, null,
              2L, 5,
              null, 6);

          armorWriter.write(myorg, table, SINGLE, Instant.now(), Arrays.asList(e11, e12, e10, e20));
          armorWriter.commit();
          armorWriter.begin();
          // Verify store/shard stuff
          List<ShardId> shardIds = writeStore.findShardIds(myorg, table, SINGLE, Instant.now(), "vuln");
          assertFalse(shardIds.isEmpty());
          ShardId shardId = shardIds.get(0);
          assertEquals(Sets.newHashSet(name, asset, vuln, time), Sets.newHashSet(writeStore.getColumnIds(shardId)));

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
          Entity deleteAsset1 = new Entity("assetId", 1, Integer.MAX_VALUE, "dkfjd;kfd");
          armorWriter.delete(myorg, table, SINGLE, Instant.now(), deleteAsset1);
          armorWriter.commit();
          armorWriter.begin();
          Map<Integer, EntityRecord> vulnEntityRecords2 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata cmd2 = armorWriter.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(1, vulnEntityRecords2.size());
//          assertEquals(Integer.valueOf(50), Integer.valueOf(cmd2.getFragmentationLevel()));
          assertEquals(Integer.valueOf(0), Integer.valueOf(cmd2.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd2.getMaxValue());
          assertEquals(Double.valueOf(5.0), cmd2.getMinValue());
          assertEquals(6, cmd2.getNumRows());
          assertEquals(1, cmd2.getNumEntities());
          //checkEntityIndexRecord(vulnEntityRecords2.get(0), 0, 24, 0, (byte) 1);
          checkEntityIndexRecord(vulnEntityRecords2.get(2), 0, 24, 15, (byte) 0);


          // Write a new entry same exact thing
          Entity e21 = Entity.buildEntity("assetId", 2, 1, null, columns);
          e21.addRows(
              "1", 6L, null,
              "1", 5L, null,
              "1", null, null,
              "1", 3L, null,
              "1", 2L, 5,
              "1", null, 6);

          armorWriter.write(myorg, table, SINGLE, Instant.now(), Collections.singletonList(e21));
          Map<Integer, EntityRecord> test1 = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "time", 0);

          armorWriter.commit();
          armorWriter.begin();

          Map<Integer, EntityRecord> test = armorWriter.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "time", 0);

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
          Entity e23 = Entity.buildEntity("assetId", 2, 3, null, columns);
          e23.addRow("1", 6L, null);
          e23.addRow("1", 5L, null);
          e23.addRow("1", null, null);
          e23.addRow("1", 3L, null);
          e23.addRow(null, null, 6);

          Entity e31 = Entity.buildEntity("assetId", 3, 1, null, columns);
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
          Entity e32 = Entity.buildEntity("assetId", 3, 2, null, columns);
          e32.addRow("1", null, 6);
          e32.addRow("1", null, -1);
          e32.addRow(null, null, null);

          ArmorWriter armorWriter2 = new ArmorWriter("test", writeStore, compression, 10, () -> 1, null);
          armorWriter2.begin();
          Map<Integer, EntityRecord> records5a = armorWriter2.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);

          armorWriter2.write(myorg, table, SINGLE, Instant.now(), Collections.singletonList(e32));
          armorWriter2.commit();
          armorWriter2.begin();;

          Map<Integer, EntityRecord> records5 = armorWriter2.columnEntityRecords(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          ColumnMetadata md5 = armorWriter2.columnMetadata(myorg, table, SINGLE, Instant.now(), "vuln", 0);
          assertEquals(2, records5.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(md5.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), md5.getMaxValue());
          assertEquals(Double.valueOf(-1.0), md5.getMinValue());
          assertEquals(8, md5.getNumRows());
          assertEquals(2, md5.getNumEntities());
          checkEntityIndexRecord(records5.get(2), 0, 20, 15, (byte) 0);
          checkEntityIndexRecord(records5.get(3), 35, 12, 18, (byte) 0);

          String entityColumnPath = PathBuilder.buildPath(myorg, table, "metadata", ColumnId.ENTITY_COLUMN_IDENTIFIER);
          List<S3ObjectSummary> entityColumnObjects = client.listObjects(new ListObjectsRequest().withBucketName(TEST_BUCKET).withPrefix(entityColumnPath)).getObjectSummaries();
          assertEquals(1, entityColumnObjects.size());
  
          armorWriter2.close(); // Close this FS and open a new one to test the load.
        } finally {
          try {
            writeStore.deleteTenant(myorg);
          } catch (Throwable t) {
            LOGGER.error("Detected an error during cleanup, will throw an error", t);
            throw t;
          }
        }
      }
    }
  }
  @Test
  public void getCachedTenantsTest() {
    String org1 = "org1";
    String org2 = "org2";
    String table = "vulntable1";
    ColumnId name = new ColumnId("name", DataType.STRING.getCode());
    ColumnId time = new ColumnId("time", DataType.LONG.getCode());
    ColumnId vuln = new ColumnId("vuln", DataType.INTEGER.getCode());
    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
    
    try (ArmorWriter armorWriter = new ArmorWriter("name", writeStore, Compression.NONE, 10, () -> 1, null)) {
      armorWriter.begin();
      Entity e11 = Entity.buildEntity("assetId", 1, 1, null, name, time, vuln);
      e11.addRows(
          "a", 6L, 1,
          "b", 5L, 2,
          "c", 4L, 3,
          "d", 3L, 4,
          "e", 2L, 5,
          "e", 2L, 5,
          "f", 1L, 6
      );
    
      Entity e12 = Entity.buildEntity("assetId", 1, 2, null, name, time, vuln);  // Should be this one
      e12.addRows(
          "a", 7L, 1,
          "b", 8L, 2,
          "c", null, 3,
          "d", 9L, 4,
          "e", 10L, 5,
          "e", 11L, 6
      );
    
      Entity e10 = Entity.buildEntity("assetId", 1, 0, null, name, time, vuln);
      e10.addRows(
          "a", 6L, null,
          "a", 5L, null,
          "a", null, null,
          "a", 3L, null,
          "a", 2L, 5,
          "a", 1L, 6);
    
      Entity e20 = Entity.buildEntity("assetId", 2, 0, null, time, vuln);
      e20.addRows(
          6L, null,
          5L, null,
          null, null,
          3L, null,
          2L, 5,
          null, 6);
    
      armorWriter.write(org1, table, SINGLE, Instant.now(), Arrays.asList(e11, e12, e10, e20));
      armorWriter.commit();
      armorWriter.begin();
      armorWriter.write(org2, table, SINGLE, Instant.now(), Arrays.asList(e11, e12, e10, e20));
      armorWriter.commit();
  
      S3ReadStore s3ReadStore = new S3ReadStore(client, TEST_BUCKET);
      List<String> tenants = s3ReadStore.getTenants(true);
      assertEquals(Arrays.asList(org1, org2), tenants);
      
      tenants = writeStore.getTenants(true);
      assertEquals(Arrays.asList(org1, org2), tenants);
    }
  }
}
