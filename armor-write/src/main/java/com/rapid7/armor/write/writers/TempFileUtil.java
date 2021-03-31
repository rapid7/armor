package com.rapid7.armor.write.writers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;

public class TempFileUtil {

  private static Path tempFileLocation = null;
  
  public static void setTempFileLocation(Path tmpFileLocation) {
    tempFileLocation = tmpFileLocation;
  }
  
  public Path getTempFileLocation() {
    if (tempFileLocation != null)
      return tempFileLocation;
    String tmpDir = System.getProperty("java.io.tmpdir");
    if (tmpDir == null)
      return null;
    return Paths.get(tmpDir);
  }

  public static Path createTempFile(String prefix, String suffix, FileAttribute<?>... attrs) throws IOException {
    if (tempFileLocation == null)
      return Files.createTempFile(prefix, suffix, attrs);
    else {
       return Files.createFile(tempFileLocation, attrs);
    }
  }
  
  private TempFileUtil() {}
}
