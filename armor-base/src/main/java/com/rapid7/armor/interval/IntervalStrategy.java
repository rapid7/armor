package com.rapid7.armor.interval;

import java.time.Instant;

public interface IntervalStrategy {
  String getInterval();
  String getIntervalStart(Instant timestamp);
  boolean supports(String interval);
}
