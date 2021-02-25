package com.rapid7.armor.interval;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class IntervalStrategyTest {
  
  @Test
  public void testWeekly() {
    Instant ld = LocalDate.parse("2021-02-22").atStartOfDay().toInstant(ZoneOffset.UTC);
    assertEquals("2021-02-21T00:00:00Z", Interval.WEEKLY.getIntervalStart(ld));
  }

  @Test
  public void testFixed() {
    Clock clock = Clock.fixed(Instant.parse("2021-01-01T00:15:00Z"), ZoneId.of("UTC"));
    IntervalStrategy intervalStrategy = new IntervalStrategyFixed(12 * 60 * 60 * 1000);

    assertEquals(Long.toString(12 * 60), intervalStrategy.getInterval());
    assertEquals("2021-01-01T00:00:00Z", intervalStrategy.getIntervalStart(Instant.now(clock)));
  }

  @Test
  public void testSingle() {
    IntervalStrategy intervalStrategy = new IntervalStrategySingle();

    assertEquals("all", intervalStrategy.getInterval());
    assertEquals("1970-01-01T00:00:00Z", intervalStrategy.getIntervalStart(Instant.now()));
  }

  @Test
  public void testMonthly() {
    Clock clock = Clock.fixed(Instant.parse("2021-03-24T01:15:05Z"), ZoneId.of("UTC"));
    IntervalStrategy intervalStrategy = new IntervalStrategyMonthly();

    assertEquals("monthly", intervalStrategy.getInterval());
    assertEquals("2021-03-01T00:00:00Z", intervalStrategy.getIntervalStart(Instant.now(clock)));
  }

  @Test
  public void testYearly() {
    Clock clock = Clock.fixed(Instant.parse("2021-03-24T01:15:05Z"), ZoneId.of("UTC"));
    IntervalStrategy intervalStrategy = new IntervalStrategyYearly();

    assertEquals("yearly", intervalStrategy.getInterval());
    assertEquals("2021-01-01T00:00:00Z", intervalStrategy.getIntervalStart(Instant.now(clock)));
  }
}
