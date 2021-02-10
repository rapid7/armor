package com.rapid7.armor.write.component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileComponentTest {

  @Test
  public void basicTests() throws IOException {
    Path path = Files.createTempFile("file-est", "test");
    try (FileComponent fc = new FileComponent(path)){
      assertEquals(Long.valueOf(0), fc.getCurrentSize());
      fc.write(new byte[] {1, 2});
      assertEquals(Long.valueOf(2), fc.getCurrentSize());
      fc.truncate(1);
      assertEquals(Long.valueOf(1), fc.getCurrentSize());
      fc.close();
    } finally {
      Files.deleteIfExists(path);
    }
  }
}
