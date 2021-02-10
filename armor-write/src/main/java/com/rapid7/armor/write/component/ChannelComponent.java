package com.rapid7.armor.write.component;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface ChannelComponent extends Component, AutoCloseable {

  long position() throws IOException;

  void position(long position) throws IOException;

  int read(ByteBuffer byteBuffer) throws IOException;

  int write(ByteBuffer byteBuffer) throws IOException;

  int write(byte[] buffer) throws IOException;

  void close() throws IOException;

  void truncate(int size) throws IOException;
}
