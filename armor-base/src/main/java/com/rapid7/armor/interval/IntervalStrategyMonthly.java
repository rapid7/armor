package com.rapid7.armor.interval;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class IntervalStrategyMonthly implements IntervalStrategy {
  private static final String INTERVAL = "monthly";

  @Override
  public String getInterval() {
    return INTERVAL;
  }

  @Override
  public String getIntervalStart(Instant timestamp) {
    ZonedDateTime dateTime = timestamp.atZone(ZoneId.of("UTC"));

    return dateTime
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
    return INTERVAL.equals(interval);
  }
}
