package com.rapid7.armor.interval;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;

public class IntervalStrategyWeekly implements IntervalStrategy {
  private static final String INTERVAL = "weekly";

  @Override
  public String getInterval() {
    return INTERVAL;
  }

  @Override
  public String getIntervalStart(Instant timestamp) {
    ZonedDateTime dateTime = timestamp.atZone(ZoneId.of("UTC"));

    return dateTime
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
        .withHour(0)
        .withMinute(0)
        .withSecond(0)
        .withNano(0)
        .toInstant()
        .toString();
  }

  @Override
  public String getIntervalStart(Instant timestamp, int offset) {
    ZonedDateTime dateTime = timestamp.atZone(ZoneId.of("UTC"));

    return dateTime
        .plusWeeks(offset)
        .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
        .withHour(0)
        .withMinute(0)
        .withSecond(0)
        .withNano(0)
        .toInstant()
        .toString();
  }

  @Override
  public boolean supports(String interval) {
    return INTERVAL.equalsIgnoreCase(interval);
  }
}
