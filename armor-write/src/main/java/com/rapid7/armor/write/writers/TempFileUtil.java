package com.rapid7.armor.write.writers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

public class TempFileUtil {

  private static Path tempFileLocation = null;
  
  public static void setTempFileLocation(Path tmpFileLocation) {
    tempFileLocation = tmpFileLocation;
  }
  
  public Path getTempFileLocation() {
    return tempFileLocation;
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
