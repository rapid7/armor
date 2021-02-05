package com.rapid7.armor.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.roaringbitmap.RoaringBitmap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.schema.ColumnName;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.ColumnWriter;
import com.rapid7.armor.write.component.DictionaryWriter;

/**
 * This analyzer will look filter out and only find entities according to how writers and readers
 * interact with the column file. This is a useful tool to see how on more entites look like from the perspective
 * of reads and writes, from both the reader and writer. You can pick more than 1 entity you are looking.
 */
public class ArmorEntityAnalyzer {

  public static void main(String[] args) throws IOException, ParseException, java.text.ParseException {
    Options options = new Options();
    options.addOption("p", "path", true, "Path to the armor file or directory");
    options.addOption("e", "entities", true, "CSV list of entities to find, can be surrogate (if applicable) or real value");
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
    String[] entities = entityArg.split(",");
    if (!Files.isDirectory(targetPath))
      throw new RuntimeException("Target should be a directory");
    
    // Try to determine the entity type.
    boolean isNumeric = true;
    for (String e : entities) {
      try {
        Integer.parseInt(e);
      } catch (NumberFormatException nfe) {
        isNumeric = false;
        break;
      }
    }
    
    Map<String, Object> inferedEntityIds = new HashMap<>();
    if (isNumeric) {
      for (String e : entities) {
        inferedEntityIds.put(e, Integer.parseInt(e));
      }
    } else {
      for (String e : entities) {
        inferedEntityIds.put(e, e);
      }
    }
    
    for (Path c : findAllFiles(path)) {
      ColumnName columnName = null;
      try {
        columnName = new ColumnName(c.getFileName().toString());
      } catch (Exception e) {
        // No valid so just skip.
        continue;
      }
      ObjectMapper om = new ObjectMapper();
      try (ColumnWriter writer = 
          new ColumnWriter(new DataInputStream(Files.newInputStream(c, StandardOpenOption.READ)), new ColumnShardId(new ShardId(1, "dummy", "dummy"), columnName))) {
        DictionaryWriter dw = writer.getEntityDictionary();
        if (dw == null) {
          if (isNumeric) {
            for (Map.Entry<String, Object> entry : inferedEntityIds.entrySet()) {
              Object key = entry.getValue();
              String entityName = entry.getKey();
              EntityRecord er = writer.getEntites().get((Integer) key);
              if (er != null) {
                // Write info
                List<Object> results = new ArrayList<>();
                writer.getRowGroupWriter().customExtractValus(er, (a) -> {
                  results.addAll(a);
                });
                
                EntityRecord writeRecord  = (EntityRecord) results.get(0);
                List<Object> writeValues = (List<Object>) results.get(1);
                RoaringBitmap writeNilBm = (RoaringBitmap) results.get(2);
                for (int v : writeNilBm.toArray()) {
                  writeValues.set(v-1, null);
                }
                Files.copy(
                    new ByteArrayInputStream(om.writeValueAsBytes(writeValues)),
                    targetPath.resolve(columnName.getName() + "-" + entityName + "-writeValues.json"),
                    StandardCopyOption.REPLACE_EXISTING);
                // If it exists here, then lets pull from the read side, for now just to the Fast.
                FastArmorShardColumn fas = new FastArmorShardColumn(Files.newInputStream(c, StandardOpenOption.READ));
                List<Object> readValues = fas.getValuesForRecord(writeRecord.getEntityId());
                ObjectMapper objectMapper = new ObjectMapper();
                Files.copy(
                    new ByteArrayInputStream(objectMapper.writeValueAsBytes(readValues)),
                    targetPath.resolve(columnName.getName() + "-" + entityName + "-readValues.json"),
                    StandardCopyOption.REPLACE_EXISTING);
              }
            }
          } else
            continue;
        } else {
          // We know its a string
          for (Map.Entry<String, Object> entry : inferedEntityIds.entrySet()) {
            String entityName = entry.getKey();
            Integer entityId = dw.getSurrogate(entityName);
            EntityRecord er = writer.getEntites().get(entityId);
            if (er != null) {
              // Write info
              List<Object> results = new ArrayList<>();
              writer.getRowGroupWriter().customExtractValus(er, (a) -> {
                results.addAll(a);
              });
              
              EntityRecord writeRecord  = (EntityRecord) results.get(0);
              List<Object> writeValues = (List<Object>) results.get(1);
              RoaringBitmap writeNilBm = (RoaringBitmap) results.get(2);
              for (int v : writeNilBm.toArray()) {
                writeValues.set(v-1, null);
              }
              Files.copy(
                  new ByteArrayInputStream(om.writeValueAsBytes(writeValues)),
                  targetPath.resolve(columnName.getName() + "-" + entityName + "-writeValues.json"),
                  StandardCopyOption.REPLACE_EXISTING);
              // If it exists here, then lets pull from the read side, for now just to the Fast.
              FastArmorShardColumn fas = new FastArmorShardColumn(Files.newInputStream(c, StandardOpenOption.READ));
              List<Object> readValues = fas.getValuesForRecord(writeRecord.getEntityId());
              ObjectMapper objectMapper = new ObjectMapper();
              Files.copy(
                  new ByteArrayInputStream(objectMapper.writeValueAsBytes(readValues)),
                  targetPath.resolve(columnName.getName() + "-" + entityName + "-readValues.json"),
                  StandardCopyOption.REPLACE_EXISTING);
              
            }
          }
        }
      }
    }
    
  }
  
  public static List<Path> findAllFiles(Path path) throws IOException {
    List<Path> allFiles = new ArrayList<>();
    Files.walk(path).filter(Files::isRegularFile).forEach(f -> allFiles.add(f.toFile().toPath()));
    return allFiles;
  }

}
