package com.rapid7.armor.write;

import java.io.InputStream;

public class StreamProduct {

  private final int byteSize;
  private final InputStream inputStream;

  public StreamProduct(int byteSize, InputStream inputStream) {
    this.inputStream = inputStream;
    this.byteSize = byteSize;
  }

  public int getByteSize() {
    return byteSize;
  }

  public InputStream getInputStream() {
    return inputStream;
  }
}
