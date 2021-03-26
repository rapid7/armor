package com.rapid7.armor;

import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.schema.DataType;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.store.Operator;
import com.rapid7.armor.store.S3ReadStore;
import com.rapid7.armor.xact.DistXact;
import com.rapid7.armor.xact.DistXactUtil;
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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.findify.s3mock.S3Mock;

public class S3ReadStoreTest {
  private static final String TEST_BUCKET = "testbucket";
  private static final S3Mock S3_MOCK = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
  private static AmazonS3 client;
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TableMetadata generateTestTableMedataDate() {
    TableMetadata tmd = new TableMetadata("org", "tenant", "columnId", "columnIdType");
    ColumnId c1 = new ColumnId("column1", DataType.STRING);
    ColumnId c2 = new ColumnId("column2", DataType.STRING);
    ColumnId c3 = new ColumnId("column3", DataType.STRING);
    tmd.addColumnIds(Arrays.asList(c1, c2, c3));
    return tmd;
  }

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
    client.shutdown();
    S3_MOCK.shutdown();
  }

  @Test
  public void readStoreTests() throws SdkClientException, JsonProcessingException {
    // First lets setup potential shards and directories in s3, directly write to s3
    String current1 = UUID.randomUUID().toString();
    ObjectMapper mapper = new ObjectMapper();
    HashMap<String, String> currentValue1 = new HashMap<>();
    currentValue1.put("current", current1);

    String current2 = UUID.randomUUID().toString();
    HashMap<String, String> currentValue2 = new HashMap<>();
    currentValue2.put("current", current2);

    TableMetadata tmdTest = generateTestTableMedataDate();
    String tmdJson = OBJECT_MAPPER.writeValueAsString(tmdTest);
    client.putObject(TEST_BUCKET, "org1/table1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
    client.putObject(TEST_BUCKET, "org1/table1/" + current1 + "/table-metadata.armor", tmdJson);
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/name_S", "Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/level_I", "Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/0/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));

    ShardId shard0Org1 = new ShardId("org1", "table1", Interval.SINGLE.getInterval(), "1970-01-01T00:00:00Z", 0);

    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org1/table1/single/1970-01-01T00:00:00Z/1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue2));
    
    ShardId shard1Org1 = new ShardId("org1", "table1", Interval.SINGLE.getInterval(), "1970-01-01T00:00:00Z", 1);

    client.putObject(TEST_BUCKET, "org2/table1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));
    client.putObject(TEST_BUCKET, "org2/table1/" + current1 + "/table-metadata.armor", tmdJson);
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/name_S", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/level_I", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/0/" + current1 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/0/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue1));

    ShardId shard0Org2 = new ShardId("org2", "table1", Interval.SINGLE.getInterval(), "1970-01-01T00:00:00Z", 0);

    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/name_S", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/level_I", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/single/1970-01-01T00:00:00Z/1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue2));
    
    ShardId shard1Org2 = new ShardId("org2", "table1", Interval.SINGLE.getInterval(), "1970-01-01T00:00:00Z", 1);

    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-04T00:00:00Z/1/" + current2 + "/name_S", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-04T00:00:00Z/1/" + current2 + "/level_I", "Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-04T00:00:00Z/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-04T00:00:00Z/1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue2));
    
    ShardId shard1Org2Week_04 = new ShardId("org2", "table1", Interval.WEEKLY.getInterval(), "2021-01-04T00:00:00Z", 1);

    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-11T00:00:00Z/1/" + current2 + "/name_S", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-11T00:00:00Z/1/" + current2 + "/level_I", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-11T00:00:00Z/1/" + current2 + "/shard-metadata.armor", " Empty content");
    client.putObject(TEST_BUCKET, "org2/table1/weekly/2021-01-11T00:00:00Z/1/" + DistXact.CURRENT_MARKER, mapper.writeValueAsString(currentValue2));

    ShardId shard1Org2Week_11 = new ShardId("org2", "table1", Interval.WEEKLY.getInterval(), "2021-01-11T00:00:00Z", 1);

    S3ReadStore readStore = new S3ReadStore(client, TEST_BUCKET);
    assertEquals(Sets.newHashSet("org1", "org2"), Sets.newHashSet(readStore.getTenants()));
    assertEquals(Arrays.asList("table1"), readStore.getTables("org2"));
    assertEquals(Arrays.asList("table1"), readStore.getTables("org1"));
    
    List<String> intervalStarts = readStore.getIntervalStarts("org2", "table1", Interval.WEEKLY);
    assertEquals(2, intervalStarts.size());
    assertTrue(intervalStarts.contains("2021-01-04T00:00:00Z"));
    assertTrue(intervalStarts.contains("2021-01-11T00:00:00Z"));
    
    Instant instant01012021 = Instant.parse("2021-01-04T00:00:00Z");
    Instant instant01082021 = Instant.parse("2021-01-08T00:00:00Z");
    Instant instant02082021 = Instant.parse("2021-02-08T00:00:00Z");

    InstantPredicate predicate = new InstantPredicate("__startInterval", Operator.EQUALS, instant01012021);
    intervalStarts = readStore.getIntervalStarts("org2", "table1", Interval.WEEKLY, predicate);
    assertEquals(1, intervalStarts.size());
    assertTrue(intervalStarts.contains("2021-01-04T00:00:00Z"));
    
    predicate = new InstantPredicate("__startInterval", Operator.EQUALS, instant02082021);
    intervalStarts = readStore.getIntervalStarts("org2", "table1", Interval.WEEKLY, predicate);
    assertEquals(0, intervalStarts.size());
    
    predicate = new InstantPredicate("__startInterval", Operator.LESS_THAN, instant02082021);
    intervalStarts = readStore.getIntervalStarts("org2", "table1", Interval.WEEKLY, predicate);
    assertEquals(2, intervalStarts.size());
    assertTrue(intervalStarts.contains("2021-01-04T00:00:00Z"));
    assertTrue(intervalStarts.contains("2021-01-11T00:00:00Z"));
    
    predicate = new InstantPredicate("__startInterval", Operator.GREATER_THAN, instant01012021);
    intervalStarts = readStore.getIntervalStarts("org2", "table1", Interval.WEEKLY, predicate);
    assertEquals(1, intervalStarts.size());
    assertTrue(intervalStarts.contains("2021-01-11T00:00:00Z"));
    
    TableMetadata tmd1 = readStore.getTableMetadata("org1", "table1");
    assertEquals(Sets.newHashSet(tmd1.getColumnIds()), Sets.newHashSet(readStore.getColumnIds("org1", "table1")));
    TableMetadata tmd2 = readStore.getTableMetadata("org1", "table1");
    assertEquals(Sets.newHashSet(tmd2.getColumnIds()), Sets.newHashSet(readStore.getColumnIds("org2", "table1")));

    List<Interval> intervals = readStore.getIntervals("org1", "table1"); 
    assertEquals(Sets.newHashSet(Interval.SINGLE), Sets.newHashSet(intervals));
    intervals = readStore.getIntervals("org2", "table1");
    assertEquals(Sets.newHashSet(Interval.WEEKLY, Interval.SINGLE), Sets.newHashSet(intervals));

    List<ShardId> shards = readStore.findShardIds("org1", "table1", Interval.SINGLE);
    assertEquals(Sets.newHashSet(shard0Org1, shard1Org1), Sets.newHashSet(shards));
    
    shards = readStore.findShardIds("org1", "table1", Interval.SINGLE);
    assertEquals(Sets.newHashSet(shard0Org1, shard1Org1), Sets.newHashSet(shards));
    
    shards = readStore.findShardIds("org2", "table1", Interval.SINGLE);
    assertEquals(Sets.newHashSet(shard0Org2, shard1Org2), Sets.newHashSet(shards));

    shards = readStore.findShardIds("org2", "table1", Interval.WEEKLY);
    assertEquals(Sets.newHashSet(shard1Org2Week_11, shard1Org2Week_04), Sets.newHashSet(shards));
    
    InstantPredicate instantPredicate1 = new InstantPredicate("", Operator.EQUALS, Instant.parse("2021-01-04T00:00:00Z"));
    shards = readStore.findShardIds("org1", "table1", Interval.SINGLE, instantPredicate1);
    assertEquals(Sets.newHashSet(shard0Org1, shard1Org1), Sets.newHashSet(shards));
    
    shards = readStore.findShardIds("org2", "table1", Interval.WEEKLY, instantPredicate1);
    assertEquals(Sets.newHashSet(shard1Org2Week_04), Sets.newHashSet(shards));
    
    InstantPredicate instantPredicateInvalid = new InstantPredicate("", Operator.EQUALS, Instant.parse("2031-01-04T00:00:00Z"));
    assertTrue(readStore.findShardIds("org2", "table1", Interval.WEEKLY, instantPredicateInvalid).isEmpty());
    
    StringPredicate noMatchStringPredicate = new StringPredicate("", Operator.EQUALS, "single123");
    assertTrue(readStore.findShardIds("org1", "table1", noMatchStringPredicate, instantPredicate1).isEmpty());

    StringPredicate singleStringPredicate = new StringPredicate("", Operator.EQUALS, "single");
    shards = readStore.findShardIds("org2", "table1", singleStringPredicate, instantPredicate1);
    assertEquals(Sets.newHashSet(shard0Org2, shard1Org2), Sets.newHashSet(shards));
    
    StringPredicate weeklyStringPredicate = new StringPredicate("", Operator.EQUALS, "weekly");
    shards = readStore.findShardIds("org2", "table1", weeklyStringPredicate, instantPredicate1);
    assertEquals(Sets.newHashSet(shard1Org2Week_04), Sets.newHashSet(shards));  
  }
}
