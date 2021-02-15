package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.shard.ShardStrategy;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.rapid7.armor.Constants.INTERVAL_UNITS;

public class FileWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriteStore.class);
  private final Path basePath;
  private final ShardStrategy shardStrategy;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FileWriteStore(Path path, ShardStrategy shardStrategy) {
    this.basePath = path;
    this.shardStrategy = shardStrategy;
  }

  @Override
  public ShardId buildShardId(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    return new ShardId(tenant, table, interval, timestamp, shardNum);
  }

  private ShardId buildShardId(String tenant, String table, long interval, Instant timestamp, String num) {
    return new ShardId(tenant, table, interval, timestamp, Integer.parseInt(num));
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, long interval, Instant timestamp, String columnId) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(tenant, table, interval, timestamp)) {
      Path currentPath = Paths.get(resolveCurrentPath(tenant, table, interval, timestamp, shardId.getShardNum()));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(currentPath)) {
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
  public List<ShardId> findShardIds(String tenant, String table, long interval, Instant timestamp) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString()));
    Set<ShardId> fileList = new HashSet<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(searchPath)) {
      for (Path path : stream) {
        if (Files.isDirectory(path)) {
          fileList.add(buildShardId(tenant, table, interval, timestamp, path.getFileName().toString()));
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }

  @Override
  public ShardId findShardId(String tenant, String table, long interval, Instant timestamp, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void saveColumn(String transaction, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    Path shardIdPath = basePath.resolve(Paths.get(columnShardId.getShardId().getShardId(), transaction, columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, StandardCopyOption.REPLACE_EXISTING);
      if (copied != byteSize) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", byteSize, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getTenant(), columnShardId.getTable(), columnShardId.getInterval(), columnShardId.getIntervalStart(), columnShardId.getShardNum());
    if (currentPath == null) {
      try {
        return new ColumnFileWriter(columnShardId);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnId().fullName()));
    try {
      if (!Files.exists(shardIdPath)) {
        Files.createDirectories(shardIdPath.getParent());
        return new ColumnFileWriter(columnShardId);
      } else {
        return new ColumnFileWriter(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), columnShardId);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public List<ColumnId> getColumnIds(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId.getTenant(), shardId.getTable(), shardId.getInterval(), shardId.getIntervalStart(), shardId.getShardNum());
    if (currentPath == null)
      return new ArrayList<>();
    Path target = Paths.get(currentPath);
    Set<ColumnId> fileList = new HashSet<>();
    try {
      Files.createDirectories(target);
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(target)) {
        for (Path path : stream) {
          if (!Files.isDirectory(path) && !path.getFileName().toString().contains(Constants.SHARD_METADATA)) {
            fileList.add(new ColumnId(path.getFileName().toString()));
          }
        }
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    return new ArrayList<>(fileList);
  }

  @Override
  public int findShardNum(Object entityId) {
    return shardStrategy.shardNum(entityId);
  }

  @Override
  public List<ColumnFileWriter> loadColumnWriters(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    String currentPath = resolveCurrentPath(tenant, table, interval, timestamp, shardNum);
    ShardId shardId = buildShardId(tenant, table, interval, timestamp, shardNum);
    List<ColumnId> columnIds = getColumnIds(buildShardId(tenant, table, interval, timestamp, shardNum));
    List<ColumnFileWriter> writers = new ArrayList<>();
    for (ColumnId columnId : columnIds) {
      Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnId.fullName()));
      try {
        if (Files.exists(shardIdPath)) {
          ColumnFileWriter writer = new ColumnFileWriter(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), new ColumnShardId(shardId, columnId));
          if (writer.getMetadata().getEntityId()) {
            writer.close();
            continue;
          }
          writers.add(writer);
        }
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return writers;
  }

  @Override
  public TableMetadata loadTableMetadata(String tenant, String table) {
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
  public void saveTableMetadata(String transaction, String tenant, String table, TableMetadata tableMetadata) {
    String relativeTarget = tenant + "/" + table + "/" + Constants.TABLE_METADATA + ".armor";
    Path target = basePath.resolve(relativeTarget);
    try {
      byte[] payload = OBJECT_MAPPER.writeValueAsBytes(tableMetadata);
      if (!Files.exists(target)) {
        Files.createDirectories(target.getParent());
        Files.write(target, payload, StandardOpenOption.CREATE_NEW);
      } else
        Files.write(target, payload, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ShardMetadata loadShardMetadata(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    String currentPath = resolveCurrentPath(tenant, table, interval, timestamp, shardNum);
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

  @Override
  public void saveShardMetadata(String transactionId, String tenant, String table, long interval, Instant timestamp, int shardNum, ShardMetadata shardMetadata) {
    ShardId shardId = buildShardId(tenant, table, interval, timestamp, shardNum);
    Path shardIdPath = basePath.resolve(Paths.get(shardId.getShardId(), transactionId, Constants.SHARD_METADATA + ".armor"));
    try {
      Files.createDirectories(shardIdPath.getParent());
      byte[] payload = OBJECT_MAPPER.writeValueAsBytes(shardMetadata);
      Files.copy(new ByteArrayInputStream(payload), shardIdPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void commit(String transaction, String tenant, String table, long interval, Instant timestamp, int shardNum) {
    Map<String, String> currentValues = getCurrentValues(tenant, table, interval, timestamp, shardNum);
    String oldCurrent = null;
    final String oldPrevious;
    if (currentValues != null) {
      oldCurrent = currentValues.get("current");
      oldPrevious = currentValues.get("previous");
    } else
      oldPrevious = null;
    if (oldCurrent != null && oldCurrent.equalsIgnoreCase(transaction))
      throw new WriteTranscationError("Create another transaction", transaction);
    saveCurrentValues(tenant, table, interval, timestamp, shardNum, transaction, oldCurrent);
    try {
      Runnable runnable = () -> {
        try {
          if (oldPrevious == null)
            return;
          Path toDelete = basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString(), Integer.toString(shardNum), oldPrevious));
          Files.walk(toDelete)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException ioe) {
          LOGGER.warn("Unable to previous shard version under {}", oldPrevious, ioe);
        }
      };
      Thread t = new Thread(runnable);
      t.start();
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", oldPrevious, e);
    }
  }

  @Override
  public String resolveCurrentPath(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    Map<String, String> values = getCurrentValues(tenant, table, interval, timestamp, shardNum);
    String current = values.get("current");
    if (current == null)
      return null;
    return basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString(), Integer.toString(shardNum), current)).toString();
  }

  @Override
  public Map<String, String> getCurrentValues(String tenant, String table, long interval, Instant timestamp, int shardNum) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString(), Integer.toString(shardNum), Constants.CURRENT));
    if (!Files.exists(searchPath))
      return new HashMap<>();
    else {
      try {
        return OBJECT_MAPPER.readValue(Files.newInputStream(searchPath), new TypeReference<Map<String, String>>() {});
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public void saveCurrentValues(String tenant, String table, long interval, Instant timestamp, int shardNum, String current, String previous) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString(), Integer.toString(shardNum), Constants.CURRENT));
    try {
      Files.createDirectories(searchPath.getParent());
      HashMap<String, String> currentValues = new HashMap<>();
      currentValues.put("current", current);
      if (previous != null)
        currentValues.put("previous", previous);
      Files.write(searchPath, OBJECT_MAPPER.writeValueAsBytes(currentValues), StandardOpenOption.CREATE);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void rollback(String transaction, String tenant, String table, long interval, Instant timestamp, int shardNum) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant, table, Long.toString(interval), timestampToIntervalStart(interval, timestamp).toString(), Integer.toString(shardNum), transaction));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", transaction, ioe);
    }
  }

  @Override
  public void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {

    Path toDelete = basePath.resolve(
        Paths.get(columnShardId.getTenant(), columnShardId.getTable(), Long.toString(columnShardId.getInterval()), columnShardId.getIntervalStart().toString(), Integer.toString(columnShardId.getShardNum()), Constants.LAST_ERROR));
    try {
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .filter(f -> !f.getName().contains(transaction))
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", toDelete, ioe);
    }

    Path shardIdPath = basePath.resolve(Paths.get(
        columnShardId.getTenant(),
        columnShardId.getTable(),
        Long.toString(columnShardId.getInterval()),
        columnShardId.getIntervalStart().toString(),
        Integer.toString(columnShardId.getShardNum()),
        Constants.LAST_ERROR,
        transaction,
        columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, StandardCopyOption.REPLACE_EXISTING);
      if (copied != size) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", size, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void captureWrites(String correlationId, ShardId shardId, List<Entity> entities, List<WriteRequest> requests, Object deleteEntity) {
  }

  @Override
  public String rootDirectory() {
    return basePath.toString();
  }

  private Instant timestampToIntervalStart(long interval, Instant timestamp) {
    return Instant.ofEpochMilli(timestamp.toEpochMilli() / (interval * INTERVAL_UNITS));
  }

  @Override
  public void deleteTenant(String tenant) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }
  }
}
