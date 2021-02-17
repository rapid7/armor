package com.rapid7.armor.interval;

import java.time.Instant;

public interface IntervalStrategy {
  String getInterval();
  String getIntervalStart(Instant timestamp);
  String getIntervalStart(Instant timestamp, int offset);
  boolean supports(String interval);
}
