package com.rapid7.armor.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class AutoDeleteFileInputStream extends FileInputStream {
  private final Path path;

  public AutoDeleteFileInputStream(Path path) throws FileNotFoundException {
    super(path.toFile());
    this.path = path;
  }

  @Override
  public void close() throws IOException {
    super.close();
    Files.deleteIfExists(path);
  }
}
