package com.rapid7.armor.interval;

import java.time.Instant;
import static com.rapid7.armor.interval.IntervalManager.REGISTERED_INTERVALS;

public enum Interval {
  HOURLY(new IntervalStrategyHourly()),
  DAILY(new IntervalStrategyDaily()),
  WEEKLY(new IntervalStrategyWeekly()),
  MONTHLY(new IntervalStrategyMonthly()),
  YEARLY(new IntervalStrategyYearly()),
  SINGLE(new IntervalStrategySingle());

  private final IntervalStrategy intervalStrategy;

  Interval(IntervalStrategy intervalStrategy) {
    this.intervalStrategy = intervalStrategy;

    REGISTERED_INTERVALS.add(this);
  }

  public String getInterval() {
    return intervalStrategy.getInterval();
  }

  public String getIntervalStart(Instant timestamp) {
    return intervalStrategy.getIntervalStart(timestamp);
  }

  public String getIntervalStart(Instant timestamp, int offset) {
    return intervalStrategy.getIntervalStart(timestamp, offset);
  }

  public static Interval toInterval(String interval) {
    for (Interval value : REGISTERED_INTERVALS) {
      if (value.intervalStrategy.supports(interval)) {
        return value;
      }
    }

    return null;
  }

}
