package com.rapid7.armor.interval;

import java.time.Instant;

public class IntervalStrategySingle implements IntervalStrategy {
  private static final String INTERVAL = "single";

  @Override
  public String getInterval() {
    return INTERVAL;
  }

  @Override
  public String getIntervalStart(Instant timestamp) {
    return Instant.ofEpochMilli(0).toString();
  }

  @Override
  public String getIntervalStart(Instant timestamp, int offset) {
    return Instant.ofEpochMilli(0).toString();
  }

  @Override
  public boolean supports(String interval) {
    return INTERVAL.equalsIgnoreCase(interval);
  }
}
