package com.rapid7.armor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.store.FileReadStore;


/**
 * Test out expected behaivor against the filesystem convention.
 * // root/tenant/table/interval/intervalStart/shards/
 */
public class FileStoreTest {
  private void setup(Path path) throws IOException {
      Path week1Shard1 = path.resolve(Paths.get("tenant", "table", "weekly", "2021-02-14T00:00:00Z", "1"));
      Files.createDirectories(week1Shard1.getParent());
      Files.createFile(week1Shard1);
      
      Path week2Shard1 = path.resolve(Paths.get("tenant", "table", "weekly", "2021-02-21T00:00:00Z", "1"));
      Files.createDirectories(week2Shard1.getParent());
      Files.createFile(week2Shard1);
      
      Path singleShard1 = path.resolve(Paths.get("tenant", "table", "single", "1970-02-21T00:00:00Z", "1"));
      Files.createDirectories(singleShard1.getParent());
      Files.createFile(singleShard1);
  }


  @Test
  public void listStartIntervals() throws IOException {
    Path testDirectory = Files.createTempDirectory("filestore");
    FileReadStore store = new FileReadStore(testDirectory);
    setup(testDirectory);
    try {
        List<String> intervalStarts = store.getIntervalStarts("tenant", "table", Interval.WEEKLY);
        assertEquals(2, intervalStarts.size());
        assertTrue(intervalStarts.contains("2021-02-14T00:00:00Z"));
        assertTrue(intervalStarts.contains("2021-02-21T00:00:00Z"));
    } finally {
        removeDirectory(testDirectory);
    }    
  }
  
  private void removeDirectory(Path removeDirectory) throws IOException {
      Files.walk(removeDirectory).filter(Files::isRegularFile).map(Path::toFile).forEach(File::delete);
      Files.walk(removeDirectory)
         .sorted(Comparator.reverseOrder())
         .map(Path::toFile)
         .filter(File::isDirectory)
         .forEach(File::delete);
  }
}
