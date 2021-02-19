package com.rapid7.armor.write;
import java.text.ParseException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.entity.Row;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.io.Compression;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ModShardStrategy;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.S3WriteStore;
import com.rapid7.armor.write.writers.ArmorWriter;

import io.findify.s3mock.S3Mock;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class IntervalTest {
  private static final String TEST_BUCKET = "testbucket";
  private static final S3Mock S3_MOCK = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
  private static AmazonS3 client;
  private static final String ASSET_ID = "assetId";
  private static Row texasVuln = new Row(1, 101l, "texas");
  private static Row caliVuln = new Row(2, 102l, "cali");

  private static List<ColumnId> COLUMNS = Arrays.asList(
      new ColumnId("status", DataType.INTEGER.getCode()),
      new ColumnId("time", DataType.LONG.getCode()),
      new ColumnId("vuln", DataType.STRING.getCode()));

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

  @Test
  public void intervalTest() {
    S3WriteStore writeStore = new S3WriteStore(client, TEST_BUCKET, new ModShardStrategy(5));
    // First 
    String org = "myorg";
    String table = "table";
    try (ArmorWriter armorWriter = new ArmorWriter("name", writeStore, Compression.ZSTD, 10, () -> 1, null)) {
      String xact = armorWriter.startTransaction();
      LocalDateTime ldt = LocalDateTime.of(2020, 12, 10, 0, 0);
      Entity decEntity = Entity.buildEntity(ASSET_ID, 1, 1, UUID.randomUUID().toString(), COLUMNS, Arrays.asList(texasVuln, caliVuln));
      armorWriter.write(xact, org, table, Interval.MONTHLY, ldt.toInstant(ZoneOffset.UTC), Arrays.asList(decEntity));
      armorWriter.commit(xact, org, table);
      
      // Verify we have a december
      List<ShardId> shards = writeStore.findShardIds(org, table, Interval.MONTHLY, ldt.toInstant(ZoneOffset.UTC));
      assertFalse(shards.isEmpty());
      
      LocalDateTime ldt2 = LocalDateTime.of(2021, 12, 10, 0, 0);
      shards = writeStore.findShardIds(org, table, Interval.MONTHLY, ldt2.toInstant(ZoneOffset.UTC));
      assertTrue(shards.isEmpty());

      // Copy with nothing
      Instant now = Instant.now();
      armorWriter.copyPreviousIntervalSliceIfNewDestination(org, table, Interval.MONTHLY, now);
      shards = writeStore.findShardIds(org, table, Interval.MONTHLY, ldt2.toInstant(ZoneOffset.UTC));
      assertTrue(shards.isEmpty());
      
      // Now try with the last interval thing
      LocalDateTime ldt3 = LocalDateTime.of(2021, 1, 12, 0, 0);
      armorWriter.copyPreviousIntervalSliceIfNewDestination(org, table, Interval.MONTHLY, ldt3.toInstant(ZoneOffset.UTC));
      shards = writeStore.findShardIds(org, table, Interval.MONTHLY, ldt3.toInstant(ZoneOffset.UTC));
      assertFalse(shards.isEmpty());
      
      // Now lets see about testing snapshotting current to a particular interval.
      // First if no current exists.
      LocalDateTime ldt4 = LocalDateTime.of(1999, 1, 12, 0, 0);
      armorWriter.snapshotCurrentToInterval(org, table, Interval.WEEKLY, ldt4.toInstant(ZoneOffset.UTC));
      shards = writeStore.findShardIds(org, table, Interval.WEEKLY, ldt4.toInstant(ZoneOffset.UTC));
      assertTrue(shards.isEmpty());
      
      
      // Now lets write to current
      xact = armorWriter.startTransaction();
      armorWriter.write(xact, org, table, Interval.SINGLE, Instant.now(), Arrays.asList(decEntity));
      armorWriter.commit(xact, org, table);

      // Ok snapshot now.
      armorWriter.snapshotCurrentToInterval(org, table, Interval.WEEKLY, ldt4.toInstant(ZoneOffset.UTC));
      shards = writeStore.findShardIds(org, table, Interval.WEEKLY, ldt4.toInstant(ZoneOffset.UTC));
      assertFalse(shards.isEmpty());
    }
  }
}
