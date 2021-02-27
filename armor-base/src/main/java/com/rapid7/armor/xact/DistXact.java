package com.rapid7.armor.xact;

public class DistXact {
  public final static String CURRENT_MARKER = "CURRENT";
  private String current;
  private String previous;
  public DistXact(String current, String previous) {
    this.current = current;
    this.previous = previous;
  }
  
  public String getCurrent() {
    return current;
  }
  
  public String getPrevious() {
    return previous;
  }
}
