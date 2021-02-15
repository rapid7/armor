package com.rapid7.armor;

import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.S3ReadStore;
import com.rapid7.armor.store.S3WriteStore;
import com.rapid7.armor.write.component.RowGroupWriter;
import com.rapid7.armor.write.writers.ArmorWriter;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
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
import org.junit.jupiter.api.Test;
import static com.rapid7.armor.Constants.MAX_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.findify.s3mock.S3Mock;

public class S3StoreTest {
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

  @AfterAll
  public static void shutdown() {
    S3_MOCK.shutdown();
  }

  private void checkEntityIndexRecord(EntityRecord eir, int rowGroupOffset, int valueLength, int nullLength, byte deleted) {
    assertEquals(valueLength, eir.getValueLength());
    assertEquals(rowGroupOffset, eir.getRowGroupOffset());
    assertEquals(nullLength, eir.getNullLength());
    assertEquals(deleted, eir.getDeleted());
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
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/0/" + current1 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/0/" + current1 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/0/" + Constants.CURRENT, mapper.writeValueAsString(currentValue1));

    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/1/" + current2 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/1/" + current2 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/1/" + Constants.CURRENT, mapper.writeValueAsString(currentValue2));

    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(2));
    ShardId shard0 = writeStore.findShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 0);
    assertEquals(0, shard0.getShardNum());
    assertEquals("org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/0", shard0.getShardId());
    ShardId shard1 = writeStore.findShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 1);
    assertEquals(1, shard1.getShardNum());
    assertEquals("org1/table1/" + MAX_INTERVAL + "/" + Instant.ofEpochMilli(0) + "/1", shard1.getShardId());


    List<ShardId> shardIds = writeStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now());
    assertTrue(shardIds.contains(shard0));
    assertTrue(shardIds.contains(shard1));

    List<ShardId> nameShardIds = writeStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now(), "name");
    assertTrue(nameShardIds.contains(shard0));
    assertTrue(nameShardIds.contains(shard1));

    List<ShardId> levelShardIds = writeStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now(), "level");
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

    assertEquals(shard0, writeStore.buildShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 0));
    assertEquals(shard1, writeStore.buildShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 1));

    S3ReadStore readStore = new S3ReadStore(client, TEST_BUCKET);
    assertEquals(shard0, readStore.findShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 0));
    assertEquals(shard1, readStore.findShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 1));
    assertNull(readStore.findShardId("org1", "table1", MAX_INTERVAL, Instant.now(), 100));

    List<ShardId> readShardIds = readStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now());
    assertTrue(readShardIds.contains(shard0));
    assertTrue(readShardIds.contains(shard1));

    List<ShardId> readNameShardIds = readStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now(), "name");
    assertTrue(readNameShardIds.contains(shard0));
    assertTrue(readNameShardIds.contains(shard1));

    List<ShardId> levelNameShardIds = readStore.findShardIds("org1", "table1", MAX_INTERVAL, Instant.now(), "level");
    assertTrue(levelNameShardIds.contains(shard0));
    assertTrue(levelNameShardIds.contains(shard1));

    List<ColumnId> shard0Columns = readStore.getColumnIds(shard0);
    assertTrue(shard0Columns.contains(nameColumn));
    assertTrue(shard0Columns.contains(levelColumn));

    List<ColumnId> shard1Columns = readStore.getColumnIds(shard1);
    assertTrue(shard1Columns.contains(nameColumn));
    assertTrue(shard1Columns.contains(levelColumn));

    List<ColumnId> tableColumns = readStore.getColumnIds("org1", "table1", MAX_INTERVAL, Instant.now());
    assertTrue(tableColumns.contains(nameColumn));
    assertTrue(tableColumns.contains(levelColumn));

    List<String> tables = readStore.getTables("org1");
    assertTrue(tables.contains("table1"));
  }

  @Test
  public void basicTests() throws Exception {
    String myorg = "myorg";
    String table = "vulntable";
    ColumnId name = new ColumnId("name", DataType.STRING.getCode());
    ColumnId time = new ColumnId("time", DataType.LONG.getCode());
    ColumnId vuln = new ColumnId("vuln", DataType.INTEGER.getCode());
    ColumnId asset = new ColumnId("asset", DataType.INTEGER.getCode());
    List<ColumnId> columns = Arrays.asList(name, time, vuln);
    for (int i = 0; i < 2; i++) {
      if (i == 1)
        RowGroupWriter.setupFixedCapacityBufferPoolSize(1);
      for (Compression compression : Compression.values()) {
        S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(1));
        try (ArmorWriter armorWriter = new ArmorWriter("name", writeStore, Compression.NONE, 10, () -> 1, null)) {
          String transaction = armorWriter.startTransaction();
          Entity e11 = Entity.buildEntity("asset", 1, 1, null, name, time, vuln);
          e11.addRows(
              "a", 6L, 1,
              "b", 5L, 2,
              "c", 4L, 3,
              "d", 3L, 4,
              "e", 2L, 5,
              "e", 2L, 5,
              "f", 1L, 6
              );

          Entity e12 = Entity.buildEntity("asset", 1, 2, null, name, time, vuln);  // Should be this one
          e12.addRows(
              "a", 7L, 1,
              "b", 8L, 2,
              "c", null, 3,
              "d", 9L, 4,
              "e", 10L, 5,
              "e", 11L, 6
              );

          Entity e10 = Entity.buildEntity("asset", 1, 0, null, name, time, vuln);
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

          armorWriter.write(transaction, myorg, table, MAX_INTERVAL, Instant.now(), Arrays.asList(e11, e12, e10, e20));
          armorWriter.commit(transaction, myorg, table);
          transaction = armorWriter.startTransaction();

          // Verify store/shard stuff
          List<ShardId> shardIds = writeStore.findShardIds(myorg, table, MAX_INTERVAL, Instant.now(), "vuln");
          assertFalse(shardIds.isEmpty());
          ShardId shardId = shardIds.get(0);
          assertEquals(Sets.newHashSet(name, asset, vuln, time), Sets.newHashSet(writeStore.getColumnIds(shardId)));

          // 12 rows, 2 entities 1 and 2, freebytes 0
          Map<Integer, EntityRecord> vulnEntityRecords1 = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          ColumnMetadata cmd1 = armorWriter.columnMetadata(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          assertEquals(2, vulnEntityRecords1.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(cmd1.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd1.getMaxValue());
          assertEquals(Double.valueOf(1.0), cmd1.getMinValue());
          assertEquals(12, cmd1.getNumRows());
          assertEquals(2, cmd1.getNumEntities());

          checkEntityIndexRecord(vulnEntityRecords1.get(1), 0, 24, 0, (byte) 0);
          checkEntityIndexRecord(vulnEntityRecords1.get(2), 24, 24, 15, (byte) 0);

          // Delete the entity 1
          armorWriter.delete(transaction, myorg, table, MAX_INTERVAL, Instant.now(), 1, Integer.MAX_VALUE, "dkfjd;kfd");
          armorWriter.commit(transaction, myorg, table);
          transaction = armorWriter.startTransaction();
          Map<Integer, EntityRecord> vulnEntityRecords2 = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          ColumnMetadata cmd2 = armorWriter.columnMetadata(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          assertEquals(2, vulnEntityRecords2.size());
          assertEquals(Integer.valueOf(50), Integer.valueOf(cmd2.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), cmd2.getMaxValue());
          assertEquals(Double.valueOf(5.0), cmd2.getMinValue());
          assertEquals(6, cmd2.getNumRows());
          assertEquals(1, cmd2.getNumEntities());
          checkEntityIndexRecord(vulnEntityRecords2.get(1), 0, 24, 0, (byte) 1);
          checkEntityIndexRecord(vulnEntityRecords2.get(2), 24, 24, 15, (byte) 0);


          // Write a new entry same exact thing
          Entity e21 = Entity.buildEntity("assetId", 2, 1, null, columns);
          e21.addRows(
              "1", 6L, null,
              "1", 5L, null,
              "1", null, null,
              "1", 3L, null,
              "1", 2L, 5,
              "1", null, 6);

          armorWriter.write(transaction, myorg, table, MAX_INTERVAL, Instant.now(), Collections.singletonList(e21));
          Map<Integer, EntityRecord> test1 = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "time", 0);

          armorWriter.commit(transaction, myorg, table);
          transaction = armorWriter.startTransaction();

          Map<Integer, EntityRecord> test = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "time", 0);

          Map<Integer, EntityRecord> vulnEntityRecords3 = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          ColumnMetadata cmd3 = armorWriter.columnMetadata(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
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

          armorWriter.write(transaction, myorg, table, MAX_INTERVAL, Instant.now(), Arrays.asList(e23, e31));
          armorWriter.commit(transaction, myorg, table);
          transaction = armorWriter.startTransaction();

          Map<Integer, EntityRecord> records4 = armorWriter.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          ColumnMetadata md4 = armorWriter.columnMetadata(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          assertEquals(2, records4.size());
          assertEquals(Integer.valueOf(58), Integer.valueOf(md4.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), md4.getMaxValue());
          assertEquals(Double.valueOf(-1.0), md4.getMinValue());
          assertEquals(7, md4.getNumRows());
          assertEquals(2, md4.getNumEntities());
          checkEntityIndexRecord(records4.get(2), 39, 20, 15, (byte) 0);
          checkEntityIndexRecord(records4.get(3), 74, 8, 0, (byte) 0);

          // Overwrite existing one but this time expand the row count
          Entity e32 = Entity.buildEntity("assetId", 3, 2, null, columns);
          e32.addRow("1", null, 6);
          e32.addRow("1", null, -1);
          e32.addRow(null, null, null);

          ArmorWriter amrorWriter2 = new ArmorWriter("test", writeStore, Compression.NONE, 10, () -> 1, null);
          Map<Integer, EntityRecord> records5a = amrorWriter2.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);

          amrorWriter2.write(transaction, myorg, table, MAX_INTERVAL, Instant.now(), Collections.singletonList(e32));
          amrorWriter2.commit(transaction, myorg, table);
          transaction = armorWriter.startTransaction();

          Map<Integer, EntityRecord> records5 = amrorWriter2.columnEntityRecords(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          ColumnMetadata md5 = amrorWriter2.columnMetadata(myorg, table, MAX_INTERVAL, Instant.now(), "vuln", 0);
          assertEquals(2, records5.size());
          assertEquals(Integer.valueOf(0), Integer.valueOf(md5.getFragmentationLevel()));
          assertEquals(Double.valueOf(6.0), md5.getMaxValue());
          assertEquals(Double.valueOf(-1.0), md5.getMinValue());
          assertEquals(8, md5.getNumRows());
          assertEquals(2, md5.getNumEntities());
          checkEntityIndexRecord(records5.get(2), 0, 20, 15, (byte) 0);
          checkEntityIndexRecord(records5.get(3), 35, 12, 18, (byte) 0);

          amrorWriter2.close(); // Close this FS and open a new one to test the load.
        } finally {
          writeStore.deleteTenant(myorg);
        }
      }
    }
  }
}
