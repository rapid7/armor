package com.rapid7.armor.shard;

import com.rapid7.armor.interval.Interval;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardIdTest {
  @Test
  public void testShardIdConstructor12HourIntervals() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    String tenant = "myorg";
    String table = "vulntable";
    Interval interval = Interval.HOURLY;
    Instant timestamp = Instant.now(clock);
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);

    String expectedShardPath = String.format(
        "%s/%s/%s/%s/%d",
        tenant,
        table,
        interval.getInterval(),
        interval.getIntervalStart(timestamp),
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval.getInterval(), shardId.getInterval());
    assertEquals(interval.getIntervalStart(timestamp), shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.shardIdPath());
  }

  @Test
  public void testShardIdConstructorDailyIntervals() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    String tenant = "myorg";
    String table = "vulntable";
    Interval interval = Interval.DAILY;
    Instant timestamp = Instant.now(clock);
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);

    String expectedShardPath = String.format(
        "%s/%s/%s/%s/%d",
        tenant,
        table,
        interval.getInterval(),
        interval.getIntervalStart(timestamp),
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval.getInterval(), shardId.getInterval());
    assertEquals(interval.getIntervalStart(timestamp), shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.shardIdPath());
  }
  
  @Test
  public void testShardIdConstructorCurrentInterval() {
    String tenant = "myorg";
    String table = "vulntable";
    Interval interval = Interval.SINGLE;
    Instant timestamp = Instant.now();
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);

    String expectedShardPath = String.format(
        "%s/%s/%s/%s/%d",
        tenant,
        table,
        interval.getInterval(),
        interval.getIntervalStart(timestamp),
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval.getInterval(), shardId.getInterval());
    assertEquals(interval.getIntervalStart(timestamp), shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.shardIdPath());
  }
  
  @Test
  public void relativeShardId() {
    Path root = Paths.get("/a/b");
//    Path columFile = Paths.get("/a/b/tenant/table/interaval/startInterval/0/trans/columnFile");
//    ShardId test = ShardId.parse(columFile, root);
//    assertEquals("tenant", test.getTenant());
//    assertEquals("table", test.getTable());
//    assertEquals("interaval", test.getInterval());
//    assertEquals("startInterval", test.getIntervalStart());
//    assertEquals(0, test.getShardNum());

  }
}
