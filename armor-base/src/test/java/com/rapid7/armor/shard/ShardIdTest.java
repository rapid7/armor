package com.rapid7.armor.shard;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

import com.rapid7.armor.Constants;

import static com.rapid7.armor.Constants.INTERVAL_UNITS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ShardIdTest {
  @Test
  public void testShardIdConstructor12HourIntervals() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    String tenant = "myorg";
    String table = "vulntable";
    long interval = (12 * 60 * 60 * 1000) / INTERVAL_UNITS; // 12 hours
    Instant timestamp = Instant.now(clock);
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval, timestamp, shardNum);

    long expectedIntervalSlice = timestamp.toEpochMilli() / (interval * INTERVAL_UNITS);
    Instant expectedIntervalStart = Instant.ofEpochMilli(expectedIntervalSlice * interval * INTERVAL_UNITS);
    String expectedShardPath = String.format(
        "%s/%s/%d/%s/%d",
        tenant,
        table,
        interval,
        expectedIntervalStart,
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval, shardId.getInterval());
    assertEquals(expectedIntervalSlice, shardId.getIntervalSlice());
    assertEquals(expectedIntervalStart, shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.getShardId());
  }

  @Test
  public void testShardIdConstructorDailyIntervals() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    String tenant = "myorg";
    String table = "vulntable";
    long interval = (24 * 60 * 60 * 1000) / INTERVAL_UNITS; // 24 hours
    Instant timestamp = Instant.now(clock);
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval, timestamp, shardNum);

    long expectedIntervalSlice = timestamp.toEpochMilli() / (interval * INTERVAL_UNITS);
    Instant expectedIntervalStart = Instant.ofEpochMilli(expectedIntervalSlice * interval * INTERVAL_UNITS);
    String expectedShardPath = String.format(
        "%s/%s/%d/%s/%d",
        tenant,
        table,
        interval,
        expectedIntervalStart,
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval, shardId.getInterval());
    assertEquals(expectedIntervalSlice, shardId.getIntervalSlice());
    assertEquals(expectedIntervalStart, shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.getShardId());
  }
  
  @Test
  public void testShardIdConstructorCurrentInterval() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    String tenant = "myorg";
    String table = "vulntable";
    long interval = Constants.MAX_INTERVAL; // 24 hours
    Instant timestamp = Instant.now();
    int shardNum = 0;

    ShardId shardId = new ShardId(tenant, table, interval, timestamp, shardNum);

    long expectedIntervalSlice = timestamp.toEpochMilli() / (interval * INTERVAL_UNITS);
    Instant expectedIntervalStart = Instant.ofEpochMilli(expectedIntervalSlice * interval * INTERVAL_UNITS);
    String expectedShardPath = String.format(
        "%s/%s/%d/%s/%d",
        tenant,
        table,
        interval,
        expectedIntervalStart,
        shardNum
    );

    assertEquals(tenant, shardId.getTenant());
    assertEquals(table, shardId.getTable());
    assertEquals(interval, shardId.getInterval());
    assertEquals(expectedIntervalSlice, shardId.getIntervalSlice());
    assertEquals(expectedIntervalStart, shardId.getIntervalStart());
    assertEquals(expectedShardPath, shardId.getShardId());
  }
}
