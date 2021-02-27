package com.rapid7.armor.xact;

public class XactError extends RuntimeException {
  private static final long serialVersionUID = 887665872580865041L;
  private String transaction;
  
  public XactError(String transaction, String message) {
    super(message);
    this.transaction = transaction;
  }
}
