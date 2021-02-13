package com.rapid7.armor.io;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;

public class FixedCapacityByteBufferPool {
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
      
    }
  }
}
