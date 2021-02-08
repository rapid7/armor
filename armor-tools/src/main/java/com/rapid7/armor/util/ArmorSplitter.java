package com.rapid7.armor.util;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.writers.ColumnFileWriter;

/**
 * Splits a given column into separated parts, whiich can be used for further analysis.
 */
public class ArmorSplitter {
  public static void main(String[] args) throws IOException, ParseException, java.text.ParseException {
    Options options = new Options();
    options.addOption("p", "path", true, "Path to the armor file or directory");
    options.addOption("d", "destination", true, "Destination for the ouptut files");

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("armor-tools", options );
      return;
    }
      
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    Path path = Paths.get(cmd.getOptionValue("p"));
    Path targetPath = Paths.get(cmd.getOptionValue("d"));
    String entityArg = cmd.getOptionValue("e");
    
    for (Path p : findAllFiles(path)) {
      ColumnId columnId = null;
      try {
        columnId = new ColumnId(p.getFileName().toString());
      } catch (Exception e) {
        // No valid so just skip.
        continue;
      }
      ObjectMapper om = new ObjectMapper();
      try (ColumnFileWriter writer = 
          new ColumnFileWriter(new DataInputStream(Files.newInputStream(p, StandardOpenOption.READ)), new ColumnShardId(new ShardId(1, "dummy", "dummy"), columnId))) {
        writer.getRowGroupWriter();
        writer.getMetadata();
        writer.getEntityRecordWriter();
        writer.getEntityDictionary();
        writer.getEntityDictionary();
      }
    }
  }
  
  public static List<Path> findAllFiles(Path path) throws IOException {
    List<Path> allFiles = new ArrayList<>();
    Files.walk(path).filter(Files::isRegularFile).forEach(f -> allFiles.add(f.toFile().toPath()));
    return allFiles;
  }
}
