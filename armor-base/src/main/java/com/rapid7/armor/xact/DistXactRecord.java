package com.rapid7.armor.xact;

import java.util.UUID;

public class DistXactRecord {
  public final static String CURRENT_MARKER = "CURRENT";
  private final static String TRANSACTION_SEPERATOR = ":";
  private String current;
  private String previous;
  private Long currentTime;
  private Long previousTime;

  public DistXactRecord(ArmorXact armorTransaction, DistXactRecord previous) {
    this.current = armorTransaction.getTarget();
    this.currentTime = armorTransaction.getTime();
    if (previous != null) {
      this.previous = previous.current;
      this.previousTime = previous.getPreviousTime();
    }
  }

  public DistXactRecord(Object current, Object currentTime, Object previous, Object previousTime) {
    this.current = current.toString();
    if (currentTime != null) {
        if (currentTime instanceof String)
            this.currentTime = Long.parseLong(currentTime.toString());
        else if (currentTime instanceof Number)
            this.currentTime = ((Number)currentTime).longValue();
          else
            throw new IllegalArgumentException("Only strings or numbers are accepted for current time");
    }
    if (previous != null)
        this.previous = previous.toString();
    if (previousTime != null) {
        if (previousTime instanceof String)
          this.previousTime = Long.parseLong(previousTime.toString());
        else if (previousTime instanceof Number)
          this.previousTime = ((Number)previousTime).longValue();
        else
          throw new IllegalArgumentException("Only strings or numbers are accepted for previous time");
    }
  }
  
  public String getCurrent() {
    return current;
  }
  
  public String getPrevious() {
    return previous;
  }
  
  public Long getCurrentTime() {
    return currentTime;
  }
  
  public Long getPreviousTime() {
    return previousTime;
  }
  
  public static ArmorXact generateNewTransaction(String transaction, DistXactRecord baseline) {
    return new ArmorXact(transaction, baseline.getCurrent(), System.currentTimeMillis());
  }

  public void validateXact(ArmorXact armorTransaction) {    
    // Ensure the begin doesn't match any previous transactions we know about.
    String begin = armorTransaction.getTarget();
    if (current.equalsIgnoreCase(begin))
      throw new XactError("Create another transaction", begin);
    if (previous != null && previous.equalsIgnoreCase(begin))
      throw new XactError("Create another transaction", begin);
    
    // Check the baseline MUST match current
    if (!current.equalsIgnoreCase(armorTransaction.getBaseline()))
      throw new XactError(armorTransaction.getBaseline(), "The baseline transaction doesn't match, another process already has written to it");
  }
}
