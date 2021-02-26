package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.read.fast.FastArmorShardColumn;
import com.rapid7.armor.read.predicate.InstantPredicate;
import com.rapid7.armor.read.predicate.StringPredicate;
import com.rapid7.armor.read.slow.SlowArmorShardColumn;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.shard.ShardId;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FileReadStore implements ReadStore {
  private final Path basePath;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FileReadStore(Path path) {
    this.basePath = path;
  }

  private ShardId buildShardId(String tenant, String table, Interval interval, Instant timestamp, int shardNum) {
    return new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);
  }

  private ShardId buildShardId(String tenant, String table, Interval interval, Instant timestamp, String shardNum) {
    return new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), Integer.parseInt(shardNum));
  }
  
  @Override
  public TableMetadata getTableMetadata(String tenant, String table) {
    String relativeTarget = tenant + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    Path target = basePath.resolve(relativeTarget);
    if (!Files.exists(target))
      return null;
    try {
      byte[] payload = Files.readAllBytes(target);
      return OBJECT_MAPPER.readValue(payload, TableMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(tenant, table, interval, timestamp)) {
      Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum()));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path)) {
            if (path.getFileName().toString().startsWith(columnId))
              shardIds.add(shardId);
          }
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return shardIds;
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp)));
    Set<ShardId> fileList = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(searchPath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          fileList.add(buildShardId(tenant, table, interval, timestamp, path.getFileName().toString()));
        }
      }
    } catch (NoSuchFileException nfe) {
      return new ArrayList<>();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }


  @Override
  public ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, int shardNum) {
    ShardId shardId = buildShardId(tenant, table, interval, timestamp, shardNum);
    Path shardIdPath = basePath.resolve(Paths.get(shardId.getShardId()));
    if (Files.exists(shardIdPath))
      return shardId;
    else
      return null;
  }

  @Override
  public SlowArmorShardColumn getSlowArmorShard(ShardId shardId, String columnId) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnId)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum()), cn.fullName());
    try {
      if (!Files.exists(shardIdPath)) {
        Files.createDirectories(shardIdPath.getParent());
        return new SlowArmorShardColumn();
      } else {
        return new SlowArmorShardColumn(
            new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum()));
    List<ColumnId> fileList = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(shardIdPath)) {
      for (Path path : stream) {
        if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
          fileList.add(new ColumnId(path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }

    return fileList;
  }

  @Override
  public FastArmorShardColumn getFastArmorShard(ShardId shardId, String columnName) {
    List<ColumnId> columnIds = getColumnIds(shardId);
    Optional<ColumnId> option = columnIds.stream().filter(c -> c.getName().equals(columnName)).findFirst();
    if (!option.isPresent())
      return null;
    ColumnId cn = option.get();
    Path shardIdPath = Paths.get(resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum()), cn.fullName());
    if (!Files.exists(shardIdPath)) {
      return null;
    } else {
      try {
        return new FastArmorShardColumn(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)));
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getTables(String tenant) {
    Path tenantPath = basePath.resolve(Paths.get(tenant));
    List<String> tables = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tenantPath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          tables.add(path.getFileName().toString());
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return tables;
  }

  @Override
  public List<ColumnId> getColumnIds(String tenant, String table, Interval interval, Instant timestamp) {
    List<ShardId> shardIds = findShardIds(tenant, table, interval, timestamp);
    if (shardIds.isEmpty())
      return new ArrayList<>();
    Set<ColumnId> columnIds = new HashSet<>();
    for (ShardId shardId : shardIds)
      columnIds.addAll(getColumnIds(shardId));
    return new ArrayList<>(columnIds);
  }

  @Override
  public List<String> getTenants() {
    File[] directories = basePath.toFile().listFiles(File::isDirectory);
    return Arrays.stream(directories).map(File::getName).collect(Collectors.toList());
  }

  @Override
  public ColumnId getColumnId(String tenant, String table, Interval interval, Instant timestamp, String columnName) {
    List<ColumnId> columnIds = getColumnIds(tenant, table, interval, timestamp);
    Optional<ColumnId> first = columnIds.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst();
    if (first.isPresent())
      return first.get();
    else
      return null;
  }

  @Override
  public ShardMetadata getShardMetadata(String tenant, String table, Interval interval, Instant timestamp, int shardNum) {
    String currentPath = resolveCurrentPath(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), shardNum);
    if (currentPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, Constants.SHARD_METADATA + ".armor"));
    if (!Files.exists(shardIdPath))
      return null;
    try {
      byte[] payload = Files.readAllBytes(shardIdPath);
      return OBJECT_MAPPER.readValue(payload, ShardMetadata.class);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private String resolveCurrentPath(String tenant, String table, String interval, String intervalStart, int shardNum) {
    Map<String, String> values = getCurrentValues(tenant, table, interval, intervalStart, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return basePath.resolve(Paths.get(tenant, table, interval, intervalStart, Integer.toString(shardNum), current)).toString();
  }

  @SuppressWarnings("unchecked")
  private Map<String, String> getCurrentValues(String tenant, String table, String interval, String intervalStart, int shardNum) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, interval, intervalStart, Integer.toString(shardNum), Constants.CURRENT));
    if (!Files.exists(searchPath))
      return new HashMap<>();
    else {
      try {
        return OBJECT_MAPPER.readValue(Files.newInputStream(searchPath), Map.class);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<String> getIntervalStarts(String tenant, String table, Interval interval) {
     Path searchPath =  basePath.resolve(Paths.get(tenant, table, interval.getInterval()));
     List<String> intervalStarts = new ArrayList<>();
     try {
      Files.walkFileTree(searchPath, new FileVisitor<Path>() {
         @Override
         public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
             return FileVisitResult.CONTINUE;
         }
 
         @Override
         public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
             return FileVisitResult.CONTINUE;
         }
 
         @Override
         public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
             return FileVisitResult.CONTINUE;
         }
 
         @Override
         public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
             int searchNameCount = searchPath.getNameCount();
             int dirNameCount = dir.getNameCount();
             if (dirNameCount == searchNameCount + 1) {
                 intervalStarts.add(dir.getFileName().toString());
             }
             return FileVisitResult.CONTINUE;
         }
       });
      return intervalStarts;
     } catch (IOException ioe) {
         throw new RuntimeException(ioe);
     }
  }

  @Override
  public List<String> getIntervalStarts(String tenant, String table, Interval interval, InstantPredicate predicate) {
      List<String> intervalStarts = getIntervalStarts(tenant, table, interval);
      List<Instant> instants = intervalStarts.stream().map(is -> Instant.parse(is)).collect(Collectors.toList());
      List<String> matches = new ArrayList<>();
      for (Instant instant : instants) {
          if (predicate.test(instant))
              matches.add(instant.toString());
      }
      return matches;
  }

  @Override
  public List<ColumnId> getColumnIds(String tenant, String table) {
      TableMetadata tm = getTableMetadata(tenant, table);
      if (tm == null) {
          Set<ColumnId> columnIds = new HashSet<>();
          for (Interval interval : getIntervals(tenant, table)) {
              if (interval == Interval.SINGLE)
                  columnIds.addAll(getColumnIds(tenant, table, interval, Instant.now()));
              else {
                  List<String> startIntervals = getIntervalStarts(tenant, table, interval);
                  if (!startIntervals.isEmpty()) {
                      columnIds.addAll(getColumnIds(tenant, table, interval, Instant.parse(startIntervals.get(0))));
                  }
              }
          }
      }
      return tm.getColumnIds();
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval) {
      if (interval == Interval.SINGLE)
          return findShardIds(tenant, table, interval, Instant.now());
      List<String> matches = getIntervalStarts(tenant, table, interval);
      List<ShardId> shardIds = new ArrayList<>();
      for (String match : matches) {
          shardIds.addAll(findShardIds(tenant, table, interval, Instant.parse(match)));
      }
      return shardIds;
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, InstantPredicate intervalStart) {
      if (interval == Interval.SINGLE)
          return findShardIds(tenant, table, interval, Instant.now());
      List<String> intervalStarts = getIntervalStarts(tenant, table, interval);
      List<Instant> instants = intervalStarts.stream().map(is -> Instant.parse(is)).collect(Collectors.toList());
      List<String> matches = new ArrayList<>();
      for (Instant instant : instants) {
          if (intervalStart == null || intervalStart.test(instant))
              matches.add(instant.toString());
      }
      
      List<ShardId> shardIds = new ArrayList<>();
      // So now we have the matching intervals, next for each interval get the shardIds
      for (String match : matches) {
          shardIds.addAll(findShardIds(tenant, table, interval, Instant.parse(match)));
      }
      return shardIds;
  }
  
  @Override
  public List<ShardId> findShardIds(String tenant, String table, StringPredicate interval, InstantPredicate intervalStart) {
      if (interval == null) {
          // This is gonna be slow but we will do it.
          List<ShardId> shardIds = new ArrayList<>();
          List<Interval> intervals = getIntervals(tenant, table);
          for (Interval inter : intervals) {
              shardIds.addAll(findShardIds(tenant, table, inter, intervalStart));
          }
          return shardIds;
      } else if (interval.getOperator() == Operator.EQUALS && interval.getValue().equalsIgnoreCase(Interval.SINGLE.getInterval()))
          return findShardIds(tenant, table, Interval.SINGLE, Instant.now());

      List<Interval> intervals = getIntervals(tenant, table);
      List<ShardId> shardIds = new ArrayList<>();
      for (Interval inter : intervals) {
          if (interval.test(inter.getInterval()))
              shardIds.addAll(findShardIds(tenant, table, inter, intervalStart));
      }
      return shardIds;
  }

  @Override
  public List<Interval> getIntervals(String tenant, String table) {
    Path tablePath = basePath.resolve(Paths.get(tenant, table));
    List<Interval> intervals = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(tablePath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
            intervals.add(Interval.toInterval(path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return intervals;
  }
}
