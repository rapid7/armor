package com.rapid7.armor.interval;

import java.time.Instant;
import java.util.regex.Pattern;

public class IntervalStrategyFixed implements IntervalStrategy {
  private static final long INTERVAL_UNITS = 60 * 1000;
  private static final Pattern INTERVAL_PATTERN = Pattern.compile("\\d+");

  private final long interval;

  public IntervalStrategyFixed(long interval) {
    this.interval = interval;
  }

  @Override
  public String getInterval() {
    return Long.toString(interval / INTERVAL_UNITS);
  }

  @Override
  public String getIntervalStart(Instant timestamp) {
    return Instant.ofEpochMilli(timestamp.toEpochMilli() / interval * interval).toString();
  }

  @Override
  public boolean supports(String interval) {
    return INTERVAL_PATTERN.matcher(interval).matches();
  }
}
