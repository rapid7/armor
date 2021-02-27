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
  
  public void validateXact(String test) {
    if (current != null && current.equalsIgnoreCase(test))
      throw new XactError("Create another transaction", test);
    if (previous != null && previous.equalsIgnoreCase(test))
      throw new XactError("Create another transaction", test);
  }
}
