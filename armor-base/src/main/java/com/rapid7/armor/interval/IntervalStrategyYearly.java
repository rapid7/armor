package com.rapid7.armor.interval;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class IntervalStrategyYearly implements IntervalStrategy {
  private static final String INTERVAL = "yearly";

  @Override
  public String getInterval() {
    return INTERVAL;
  }

  @Override
  public String getIntervalStart(Instant timestamp) {
    ZonedDateTime dateTime = timestamp.atZone(ZoneId.of("UTC"));

    return dateTime
        .withMonth(1)
        .withDayOfMonth(1)
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
        .plusYears(offset)
        .withMonth(1)
        .withDayOfMonth(1)
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
