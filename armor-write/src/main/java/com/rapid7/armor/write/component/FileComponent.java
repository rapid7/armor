package com.rapid7.armor.write.component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileComponent implements ChannelComponent {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileComponent.class);
  private Path path;
  private FileChannel fileChannel;

  public FileComponent(Path path) {
    this.path = path;
    try {
      this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void truncate(int size) throws IOException {
    fileChannel.truncate(size);
  }

  /**
   * Rebase the path for this component to the given path.
   *
   * @param path The path to rebase to.
   */
  public void rebase(Path path) {
    // Execute a rebase where
    boolean success = false;
    FileChannel oldChannel = fileChannel;
    Path oldPath = this.path;
    try {
      fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
      fileChannel.position(Files.size(path));
      this.path = path;
      success = true;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } finally {
      if (!success)
        fileChannel = oldChannel;
      else {
        try {
          oldChannel.close();
          Files.deleteIfExists(oldPath);
        } catch (IOException ioe) {
          // do nothing
        }
      }
    }
  }

  public Path getPath() {
    return path;
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return Files.newInputStream(path, StandardOpenOption.READ);
  }

  @Override
  public long getCurrentSize() throws IOException {
    return Files.size(path);
  }

  @Override
  public long position() throws IOException {
    return fileChannel.position();
  }

  @Override
  public void position(long position) throws IOException {
    fileChannel.position(position);
  }


  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    return fileChannel.read(byteBuffer);
  }

  @Override
  public int write(byte[] buffer) throws IOException {
    ReadableByteChannel rbc = Channels.newChannel(new ByteArrayInputStream(buffer));
    return (int) this.fileChannel.transferFrom(rbc, 0, buffer.length);
  }
  
  @Override
  public int write(ByteBuffer byteBuffer) throws IOException {
    return fileChannel.write(byteBuffer);
  }

  @Override
  public void close() throws IOException {
    fileChannel.close();
    Files.deleteIfExists(path);
  }
}
