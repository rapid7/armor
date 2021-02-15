package com.rapid7.armor.schema;

import java.time.Instant;

public class Interval {
  public static final long INTERVAL_UNITS = 60 * 1000;

  public static final long DAILY_INTERVAL = (24 * 60 * 60 * 1000L) / INTERVAL_UNITS;
  public static final long WEEKLY_INTERVAL = (7 * 24 * 60 * 60 * 1000L) / INTERVAL_UNITS;
  public static final long DAYS_30_INTERVAL = (30 * 24 * 60 * 60 * 1000L) / INTERVAL_UNITS;
  public static final long DAYS_365_INTERVAL = (365 * 24 * 60 * 60 * 1000L) / INTERVAL_UNITS;
  public static final long MAX_INTERVAL = Long.MAX_VALUE / INTERVAL_UNITS;

  public static Instant timestampToIntervalStart(long interval, Instant timestamp) {
    return Instant.ofEpochMilli(timestamp.toEpochMilli() / (interval * INTERVAL_UNITS));
  }
}
