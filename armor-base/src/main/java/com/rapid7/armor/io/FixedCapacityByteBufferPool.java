package com.rapid7.armor.io;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedCapacityByteBufferPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(FixedCapacityByteBufferPool.class);
  private Deque<ByteBuffer> pool = new ArrayDeque<>();
  private int capacity;
  
  public int currentSize() {
    return pool.size();
  }

  public FixedCapacityByteBufferPool(int capacity) {
    this.capacity = capacity;
  }
  
  public synchronized ByteBuffer get() {
      ByteBuffer buffer = pool.pollFirst();
      if (buffer == null)
          buffer = ByteBuffer.allocate(capacity);
      return buffer;
  }

  public synchronized void release(ByteBuffer buffer) {
    try {
      buffer.clear();
      pool.addLast(buffer);
    } catch (Exception e) {
      LOGGER.error("Unable to return bytebuffer to pool", e);
    }
  }
}
