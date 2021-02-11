package com.rapid7.armor.io;

public enum Compression {
  NONE,
  ZSTD;
  
  public static Compression getCompression(String value) {
    for (Compression compression : Compression.values()) {
      if (compression.name().equalsIgnoreCase(value))
        return compression;
    }
    return null;
  }
}
