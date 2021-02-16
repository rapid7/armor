package com.rapid7.armor.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.component.DictionaryWriter;
import com.rapid7.armor.write.writers.ColumnFileWriter;

import tech.tablesaw.api.Table;

/**
 * Compares two columns as a writer for analysis, note this should be two columns of the same type but different versions.
 */
public class ArmorAnalyzer {
  public static void main(String[] args) throws IOException, ParseException {
    Options options = new Options();
    options.addOption("p", "path", true, "Path to the armor directory");
    options.addOption("m", "mode", true, "Reader or writer mode each output has different set of files it pushes out");
    options.addOption("d", "destination", true, "Destination directory for the ouptut files");

    if (args.length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("armor-tools", options );
      return;
    }
      
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    Path path = Paths.get(cmd.getOptionValue("p"));
    Path targetPath = Paths.get(cmd.getOptionValue("d"));
    if (!Files.isDirectory(targetPath))
      throw new RuntimeException("Target should be a directory");
    String mode = cmd.getOptionValue("m");
    if (mode.equals("write")) {
      
      for (Path columnFile : listFiles(path)) {
        ColumnId columnId = null;
        ShardId shardId = null;
        try {
          columnId = new ColumnId(columnFile);
          shardId = new ShardId(columnFile);
        } catch (Exception e) {
          continue;
          // Just skip
        }
        try (
            ColumnFileWriter writer = new ColumnFileWriter(
                new DataInputStream(Files.newInputStream(columnFile, StandardOpenOption.READ)),
                new ColumnShardId(shardId, columnId)
            )
        ) {
          ObjectMapper objectMapper = new ObjectMapper();
          Files.copy(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(writer.getMetadata())),
              targetPath.resolve(columnId.getName() + "-metadata.json"),
              StandardCopyOption.REPLACE_EXISTING);
          DictionaryWriter entityDictionaryWriter = writer.getEntityDictionary();
          Files.copy(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(entityDictionaryWriter.getStrToInt())),
              targetPath.resolve(columnId.getName() + "-entity-dictionary-str2Int.json"),
              StandardCopyOption.REPLACE_EXISTING);
          Files.copy(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(entityDictionaryWriter.getIntToStr())),
              targetPath.resolve(columnId.getName() + "-entity-dictionary-int2Str.json"),
              StandardCopyOption.REPLACE_EXISTING);
          Files.copy(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(writer.getEntityRecordSummaries())),
              targetPath.resolve(columnId.getName() + "-ordered-entity-summaries.json"),
              StandardCopyOption.REPLACE_EXISTING);
          Files.copy(
              new ByteArrayInputStream(objectMapper.writeValueAsBytes(writer.getEntites())),
              targetPath.resolve(columnId.getName() + "-raw-entity-map.json"),
              StandardCopyOption.REPLACE_EXISTING);
          
          DictionaryWriter rgDict = writer.getRowGroupWriter().getDictionaryWriter();
          if (rgDict != null) {
            Files.copy(
                new ByteArrayInputStream(objectMapper.writeValueAsBytes(rgDict.getStrToInt())),
                targetPath.resolve("rowgroup-dictionary-str2Int.json"),
                StandardCopyOption.REPLACE_EXISTING);
            Files.copy(
                new ByteArrayInputStream(objectMapper.writeValueAsBytes(rgDict.getIntToStr())),
                targetPath.resolve("rowgroup-dictionary-int2Str.json"),
                StandardCopyOption.REPLACE_EXISTING);
          }
          
          // Finally lets dump the output of the shard out into something we can consume visually
          ColumnShardVisualReader reader = new ColumnShardVisualReader(columnId, targetPath);
          reader.process(writer);
        }
      }
    } else if (mode.equals("read")) {
      Table table = Table.create("table-result");
      for (Path columnFile : listFiles(path)) {
        try {
          SlowArmorShardColumn armorReader = new SlowArmorShardColumn(Files.newInputStream(columnFile, StandardOpenOption.READ));
          table.addColumns(armorReader.getColumn());
        } catch (Exception e) {
          continue;
        }
      }
      Files.copy(
          new ByteArrayInputStream(table.printAll().getBytes()),
          targetPath.resolve("table-result"),
          StandardCopyOption.REPLACE_EXISTING);
    } else
      throw new RuntimeException("The mode " + mode + " is not supported");
    System.out.println("All done!!!");
    System.exit(0);
  }
  
  public static Set<Path> listFiles(Path dir) throws IOException {
    try (Stream<Path> stream = Files.list(dir)) {
        return stream
          .filter(file -> !Files.isDirectory(file))
          .collect(Collectors.toSet());
    }
  }
}
