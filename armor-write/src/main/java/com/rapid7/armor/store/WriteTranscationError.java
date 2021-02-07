package com.rapid7.armor.store;

public class WriteTranscationError extends RuntimeException {
  private static final long serialVersionUID = 887665872580865041L;
  private String transaction;
  
  public WriteTranscationError(String transaction, String message) {
    super(message);
    this.transaction = transaction;
  }
}
