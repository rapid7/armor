package com.rapid7.armor.io;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOTools {
  private static int BUFFER_SIZE = 1000000;
  public static int readFully(InputStream in, byte[] buf, int off, int len)
      throws IOException {
    if (off < 0)
      throw new IllegalArgumentException("invalid offset: " + off);

    if (len < 0 || (off + len) > buf.length)
      throw new IllegalArgumentException("illegal length: " + len);

    int bytesRead = 0;
    int bytesRemaining = len;

    while (bytesRemaining > 0) {
      int read = in.read(buf, off + bytesRead, bytesRemaining);

      if (read < 0)
        throw new EOFException("end of stream reached: only " + bytesRead + " out of " + len + " bytes read");

      bytesRead += read;
      bytesRemaining -= read;
    }
    return bytesRead;
  }

  public static long skipFully(InputStream in, long len)
      throws IOException {
    if (len < 0L)
      throw new IllegalArgumentException("illegal length: " + len);

    long bytesSkipped = 0L;
    long bytesRemaining = len;

    while (bytesRemaining > 0L) {
      long skipped = in.skip(bytesRemaining);

      if (skipped < 0L)
        throw new EOFException("end of stream reached");

      bytesSkipped += skipped;
      bytesRemaining -= skipped;
    }
    return bytesSkipped;
  }

  public static long copy(InputStream from, OutputStream to) throws IOException {
    checkNotNull(from);
    checkNotNull(to);
    byte[] buf = new byte[BUFFER_SIZE];
    long total = 0;
    while (true) {
      int r = from.read(buf);
      if (r == -1) {
        break;
      }
      to.write(buf, 0, r);
      total += r;
    }
    return total;
  }

  private static <T> T checkNotNull(T reference) {
    if (reference == null) {
      throw new NullPointerException();
    }
    return reference;
  }

  public static byte[] toByteArray(short value) {
    return new byte[] {(byte) (value >> 8), (byte) value};
  }

  public static byte[] toByteArray(int value) {
    return new byte[] {
        (byte) (value >> 24), (byte) (value >> 16), (byte) (value >> 8), (byte) value
    };
  }
}
