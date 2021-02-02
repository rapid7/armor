package com.rapid7.armor.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.codehaus.plexus.util.FileUtils;
import org.codehaus.plexus.util.IOUtil;

public class MakeRealExecutable {
  private static String flags = "";

  public static void main(String[] args) throws IOException {
      String workingDir = args[0];
      String source = args[1];
      String target = args[2];
      File sourceFile = new File(workingDir + "/" + source);
      File targetFile = new File(workingDir + "/" + target);

      if (!Files.exists(sourceFile.toPath()))
        throw new RuntimeException("The expected source of " + sourceFile.getAbsolutePath() + " doesn't exist");
      
      FileUtils.copyFile(sourceFile, targetFile);
      makeExecutable(targetFile);
  }

  private static void makeExecutable(File file) throws IOException {
    Path original = Paths.get(file.getAbsolutePath() + ".rx-orig");
    Files.move(file.toPath(), original);
    try (final FileOutputStream out = new FileOutputStream(file); final InputStream in = Files.newInputStream(original)) {
      out.write(("#!/bin/sh\n\nexec java " + flags + " -jar \"$0\" \"$@\"\n\n").getBytes("ASCII"));
      IOUtil.copy(in, out);
    } finally {
      Files.deleteIfExists(original);
    }
    file.setExecutable(true, false);
  }
}
