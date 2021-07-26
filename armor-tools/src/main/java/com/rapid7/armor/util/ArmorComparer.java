package com.rapid7.armor.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.roaringbitmap.RoaringBitmap;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rapid7.armor.entity.EntityRecord;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.write.component.DictionaryWriter;
import com.rapid7.armor.write.writers.ColumnFileWriter;

/**
 * Compares two column files to determine if there are any differences between the two, comparisons are done with two files of the same
 * column name in the same directory or multiple directories witin a directory.
 * Based off the metadata it will know which one is previous and which one is after. Any columns without a
 * corresponding pair will be ignored. If a there are more than 2 versions of the column in the same directory, then it will compare the oldest
 * version vs. the most recent.
 *
 * This tool can be useful to find out changes between one version of a column to another. Differences are sorted out into 3 categories
 * 
 * 1) New entities
 * 2) Deleted entities (hard or soft deleted)
 * 3) Update entities
 * 
 * It takes a look at various parts of the column file to find which parts need to be updated. This comparer is using code from the write package.
 */
public class ArmorComparer {
  public static void main(String[] args) throws IOException, ParseException, java.text.ParseException {
    Options options = new Options();
    options.addOption("p", "p", true, "Path to the armor file or directory");
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
    if (!Files.isDirectory(targetPath))
      throw new RuntimeException("Target should be a directory");
    
    // First thing is to find, group column files and pair them up for comparison.
    Set<Path> paths = listFiles(path);
    Map<ColumnId, ColumnMinMax> detectedColumns = new HashMap<>();  
    for (Path p : paths) {
      ColumnId columnId = null;
      ShardId shardId = null;
      try {
        columnId = new ColumnId(p);
        shardId = new ShardId(p);
      } catch (Exception e) {
        // No valid so just skip.
        continue;
      }
      try (ColumnFileWriter writer = 
          new ColumnFileWriter(new DataInputStream(Files.newInputStream(p, StandardOpenOption.READ)), new ColumnShardId(shardId, columnId))) {
         // Its valid, extract the date
        String lastUpdate = writer.getMetadata().getLastUpdate();
        SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
        Date date  = sdf.parse(lastUpdate);
        
        ColumnMinMax minMax = detectedColumns.get(columnId);
        if (minMax == null) {
          minMax = new ColumnMinMax();
          minMax.columnShardId = new ColumnShardId(shardId, columnId);
          minMax.max = date.toInstant();
          minMax.min = date.toInstant();
          minMax.maxPath = p;
          minMax.minPath = p;
          detectedColumns.put(columnId, minMax);
        } else {
          if (minMax.max.isBefore(date.toInstant())) {
            minMax.max = date.toInstant();
            minMax.maxPath = p;
          } else if (minMax.min.isAfter(date.toInstant())) {
            minMax.min = date.toInstant();
            minMax.minPath = p;
          }
        }
      }
    }
    // Columns are now detected, filter out the ones with nothing to compare.
    List<ColumnMinMax> columns = detectedColumns.values().stream().filter(c -> c.hasTwoPaths()).collect(Collectors.toList());
    
    // Now lets start with the comparison logic.
    // First we will start off with the differences from a write perspective.
    for (ColumnMinMax columnMinMax : columns) {
      Path maxPath = columnMinMax.maxPath;
      Path minPath = columnMinMax.minPath;
      ColumnShardId dummyShard = new ColumnShardId(columnMinMax.columnShardId.getShardId(), columnMinMax.columnShardId.getColumnId());
      try (ColumnFileWriter maxWriter = new ColumnFileWriter(new DataInputStream(Files.newInputStream(maxPath, StandardOpenOption.READ)), dummyShard);
           ColumnFileWriter minWriter = new ColumnFileWriter(new DataInputStream(Files.newInputStream(minPath, StandardOpenOption.READ)), dummyShard)) {
        Map<Integer, EntityRecord> minRecords = minWriter.getEntities();
        Map<Integer, EntityRecord> maxRecords = maxWriter.getEntities();
        Set<Integer> minRecordsInt = minRecords.keySet();
        Set<Integer> maxRecordsInt = maxRecords.keySet();
        Set<Integer> removedEntitySurrogates = minRecords.keySet().stream().filter(k -> !maxRecordsInt.contains(k)).collect(Collectors.toSet());
        Set<Integer> addedEntitySurrogates = maxRecords.keySet().stream().filter(k -> !minRecordsInt.contains(k)).collect(Collectors.toSet());
        
        // For each of the the entities, go ahead and pull the information we have about it.
        Set<EntityRecord> removeEntities = new HashSet<>();
        Set<EntityRecord> addedEntities = new HashSet<>();
        for (Integer removed :removedEntitySurrogates) {
          removeEntities.add(minRecords.get(removed));
        }
        for (Integer added : addedEntitySurrogates) {
          addedEntities.add(maxRecords.get(added));
        }
        
        // If any of the values exists then possibly corruption of the dictionary
        List<EntityRecord> changedEntitiesA = new ArrayList<>();
        List<EntityRecord> changedEntitiesB = new ArrayList<>();

        for (Map.Entry<Integer, EntityRecord> entry : minRecords.entrySet()) {
          Integer key = entry.getKey();
          EntityRecord erA = entry.getValue();
          EntityRecord erB = maxRecords.get(key);
          if (erB == null)
            continue;
          // Do a comparison but only if it changed
          if (!erA.equals(erB)) {
            changedEntitiesA.add(erA);
            changedEntitiesB.add(erB);
          }
        }        
        
        // Lets show changes values between the before and after
        Map<Integer, List<Object>> surrogateIdsToValuesA = new HashMap<>();
        maxWriter.getRowGroupWriter().customTraverseThoughValues(changedEntitiesA, (a) -> {
          boolean isDeadSpace = (Boolean) a.get(0);
          if (isDeadSpace)
            return;
          EntityRecord er = (EntityRecord) a.get(1);
          List<Object> values = (List<Object>) a.get(2);
          RoaringBitmap roarBitMap = (RoaringBitmap) a.get(3);
          for (int rowNum : roarBitMap.toArray()) {
            values.set(rowNum-1, null);
          }
          surrogateIdsToValuesA.put(er.getEntityId(), values);
        });

        Map<Integer, List<Object>> surrogateIdsToValuesB = new HashMap<>();
        maxWriter.getRowGroupWriter().customTraverseThoughValues(changedEntitiesB, (a) -> {
          boolean isDeadSpace = (Boolean) a.get(0);
          if (isDeadSpace)
            return;
          EntityRecord er = (EntityRecord) a.get(1);
          List<Object> values = (List<Object>) a.get(2);
          RoaringBitmap roarBitMap = (RoaringBitmap) a.get(3);
          for (int rowNum : roarBitMap.toArray()) {
            values.set(rowNum-1, null);
          }
          surrogateIdsToValuesB.put(er.getEntityId(), values);
        });
        
        List<ChangedEntity> changedEntities = new ArrayList<>();
        DictionaryWriter dw = minWriter.getEntityDictionary();
        for (Map.Entry<Integer, List<Object>> entry : surrogateIdsToValuesA.entrySet()) {
          Integer entityId = entry.getKey();
          if (dw != null) {
            String realId = dw.getValue(entityId);
            ChangedEntity ce = new ChangedEntity(realId, entityId, entry.getValue(), surrogateIdsToValuesB.get(entityId));
            changedEntities.add(ce);
          } else {
            ChangedEntity ce = new ChangedEntity(entityId, null, entry.getValue(), surrogateIdsToValuesB.get(entityId));
            changedEntities.add(ce);
          }
        }
        
        // Now let write out the values to disk.
        ObjectMapper objectMapper = new ObjectMapper();
        Files.copy(
            new ByteArrayInputStream(objectMapper.writeValueAsBytes(removeEntities)),
            targetPath.resolve(columnMinMax.columnShardId.getColumnId().getName() + "-removed-entities.json"),
            StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
            new ByteArrayInputStream(objectMapper.writeValueAsBytes(addedEntities)),
            targetPath.resolve(columnMinMax.columnShardId.getColumnId().getName() + "-added-entities.json"),
            StandardCopyOption.REPLACE_EXISTING);
        Files.copy(
            new ByteArrayInputStream(objectMapper.writeValueAsBytes(changedEntities)),
            targetPath.resolve(columnMinMax.columnShardId.getColumnId().getName() + "-changed-entities.json"),
            StandardCopyOption.REPLACE_EXISTING);
      }
    }    
  }
  

  @JsonInclude(value = Include.NON_NULL)
  public static class ChangedEntity {
    private Object entityId;
    private Integer surrogate;
    private List<Object> previous;
    private List<Object> after;
    public ChangedEntity() {}

    public ChangedEntity(Object entityId, Integer surrogate, List<Object> previous, List<Object> after) {
      this.entityId = entityId;
      this.surrogate = surrogate;
      this.previous = previous;
      this.after = after;
    }
    
    public Object getEntityId() {
      return this.entityId;
    }
    
    public Integer getSurrogate() {
      return this.surrogate;
    }
    
    public List<Object> getPrevious() {
      return previous;
    }
    
    public List<Object> getAfter() {
      return after;
    }
  }
  private static class ColumnMinMax {
     public ColumnShardId columnShardId;
     public Instant max;
     public Instant min;
     public Path maxPath;
     public Path minPath;
     
     public boolean hasTwoPaths() {
       if (maxPath != null && minPath != null && !maxPath.equals(minPath)) {
         return true;
       }
       return false;
     }
  }
  
  public static Set<Path> listFiles(Path dir) throws IOException {
    try (Stream<Path> stream = Files.list(dir)) {
        return stream
          .filter(file -> !Files.isDirectory(file))
          .collect(Collectors.toSet());
    }
  }
}
