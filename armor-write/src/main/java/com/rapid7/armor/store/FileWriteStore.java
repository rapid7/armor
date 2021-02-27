package com.rapid7.armor.store;

import com.rapid7.armor.Constants;
import com.rapid7.armor.columnfile.ColumnFileReader;
import com.rapid7.armor.entity.Entity;
import com.rapid7.armor.interval.Interval;
import com.rapid7.armor.meta.ColumnMetadata;
import com.rapid7.armor.meta.ShardMetadata;
import com.rapid7.armor.meta.TableMetadata;
import com.rapid7.armor.schema.ColumnId;
import com.rapid7.armor.shard.ColumnShardId;
import com.rapid7.armor.shard.ShardId;
import com.rapid7.armor.shard.ShardStrategy;
import com.rapid7.armor.write.WriteRequest;
import com.rapid7.armor.write.writers.ColumnFileWriter;
import com.rapid7.armor.xact.DistXact;
import com.rapid7.armor.xact.DistXactUtil;
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
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class FileWriteStore implements WriteStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileWriteStore.class);
  private final Path basePath;
  private final ShardStrategy shardStrategy;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public FileWriteStore(Path path, ShardStrategy shardStrategy) {
    this.basePath = path;
    this.shardStrategy = shardStrategy;
  }

  private ShardId buildShardId(String tenant, String table, Interval interval, Instant timestamp, String num) {
    return new ShardId(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp), Integer.parseInt(num));
  }

  @Override
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp, String columnId) {
    List<ShardId> shardIds = new ArrayList<>();
    for (ShardId shardId : findShardIds(tenant, table, interval, timestamp)) {
      Path currentPath = Paths.get(resolveCurrentPath(shardId));
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
  public List<ShardId> findShardIds(String tenant, String table, Interval interval, Instant timestamp) {
    Path searchPath = basePath.resolve(Paths.get(tenant, table, interval.getInterval(), interval.getIntervalStart(timestamp)));
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
  public ShardId findShardId(String tenant, String table, Interval interval, Instant timestamp, Object entityId) {
    int shardNum = shardStrategy.shardNum(entityId);
    return ShardId.buildShardId(tenant, table, interval, timestamp, shardNum);
  }

  @Override
  public void saveColumn(String transaction, ColumnShardId columnShardId, int byteSize, InputStream inputStream) {
    Path shardIdPath = basePath.resolve(Paths.get(columnShardId.getShardId().shardIdPath(), transaction, columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, REPLACE_EXISTING);
      if (copied != byteSize) {
        LOGGER.warn("Expected to write {} but confirmed only {} bytes were copied", byteSize, copied);
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public ColumnFileWriter loadColumnWriter(ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getShardId());
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
    String currentPath = resolveCurrentPath(shardId);
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
  public List<ColumnFileWriter> loadColumnWriters(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId);
    List<ColumnId> columnIds = getColumnIds(shardId);
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
  public TableMetadata getTableMetadata(String tenant, String table) {
    String relativeTarget = resolveCurrentPath(tenant, table) + "/" + Constants.TABLE_METADATA + ".armor";
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
  public void saveTableMetadata(String transaction, TableMetadata tableMetadata) {
    DistXact status = getCurrentValues(tableMetadata.getTenant(), tableMetadata.getTable());
    if (status != null && status.getCurrent().equalsIgnoreCase(transaction))
      throw new RuntimeException("Create another transaction");
    String targetTableMetaaPath = tableMetadata.getTenant() + "/" + tableMetadata.getTable() + "/" + transaction + "/" + Constants.TABLE_METADATA + ".armor";

    Path target = basePath.resolve(targetTableMetaaPath);
    try {
      byte[] payload = OBJECT_MAPPER.writeValueAsBytes(tableMetadata);
      if (!Files.exists(target)) {
        Files.createDirectories(target.getParent());
        Files.write(target, payload, StandardOpenOption.CREATE_NEW);
      } else
        Files.write(target, payload, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
      saveCurrentValues(tableMetadata.getTenant(), tableMetadata.getTable(), transaction, status == null ? null : status.getCurrent());
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    if (status == null || status.getPrevious() == null)
      return;
    try {
      String deleteTableMetaaPath = tableMetadata.getTenant() + "/" + tableMetadata.getTable() + "/" + status.getPrevious() + "/" + Constants.TABLE_METADATA + ".armor";
      Files.deleteIfExists(basePath.resolve(deleteTableMetaaPath));
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", status.getPrevious(), e);
    }

  }

  @Override
  public ShardMetadata getShardMetadata(ShardId shardId) {
    String currentPath = resolveCurrentPath(shardId);
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
  public void saveShardMetadata(String transaction, ShardMetadata shardMetadata) {
    ShardId shardId = shardMetadata.getShardId();
    Path shardIdPath = basePath.resolve(Paths.get(shardId.shardIdPath(), transaction, Constants.SHARD_METADATA + ".armor"));
    try {
      Files.createDirectories(shardIdPath.getParent());
      byte[] payload = OBJECT_MAPPER.writeValueAsBytes(shardMetadata);
      Files.copy(new ByteArrayInputStream(payload), shardIdPath, REPLACE_EXISTING);
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void copyShard(ShardId shardIdDst, ShardId shardIdSrc) {
    if (shardIdDst.equals(shardIdSrc)) {
      return;
    }

    File shardIdDstDirectory = new File(basePath.resolve(Paths.get(shardIdDst.shardIdPath())).toString());
    if (shardIdDstDirectory.exists()) {
      return;
    }

    File shardIdSrcDirectory = new File(basePath.resolve(Paths.get(shardIdSrc.shardIdPath())).toString());
    if (!shardIdSrcDirectory.exists() || !shardIdSrcDirectory.isDirectory()) {
      return;
    }

    File copying = new File(basePath.resolve(Paths.get(shardIdDst.shardIdPath(), "COPYING")).toString());
    try {
      Files.createDirectories(shardIdDstDirectory.toPath());
      copying.createNewFile();
      copyDirectory(shardIdSrcDirectory.toPath(), shardIdDstDirectory.toPath());
    } catch (Exception exception) {
      if (shardIdDstDirectory.exists()) {
        deleteDirectory(shardIdDstDirectory.toPath());
      }

      throw new RuntimeException(exception);
    } finally {
      if (copying.exists()) {
        copying.delete();
      }
    }
  }

  @Override
  public void commit(String transaction, ShardId shardId) {
    DistXact status = getCurrentValues(shardId);
    if (status != null && status.getCurrent().equalsIgnoreCase(transaction))
      throw new WriteTranscationError("Create another transaction", transaction);
    saveCurrentValues(shardId, transaction, status == null ? null : status.getCurrent());
    try {
      Runnable runnable = () -> {
        try {
          if (status == null || status.getPrevious() == null)
            return;
          Path toDelete = basePath.resolve(
             Paths.get(shardId.shardIdPath(), status.getPrevious()));
          Files.walk(toDelete)
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException ioe) {
          LOGGER.warn("Unable to previous shard version under {}", status.getPrevious(), ioe);
        }
      };
      Thread t = new Thread(runnable);
      t.start();
    } catch (Exception e) {
      LOGGER.warn("Unable to previous shard version under {}", status.getPrevious(), e);
    }
  }

  @Override
  public void rollback(String transaction, ShardId shardId) {
    try {
      Path toDelete = basePath.resolve(Paths.get(shardId.shardIdPath(), transaction));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (IOException ioe) {
      LOGGER.warn("Unable to previous shard version under {}", transaction, ioe);
    }
  }
  
  @Override
  public List<String> getTenants() {
    File[] directories = basePath.toFile().listFiles(File::isDirectory);
    return Arrays.stream(directories).map(File::getName).collect(Collectors.toList());
  }

  @Override
  public void saveError(String transaction, ColumnShardId columnShardId, int size, InputStream inputStream, String error) {

    Path toDelete = basePath.resolve(
        Paths.get(columnShardId.getTenant(), columnShardId.getTable(), columnShardId.getInterval(), columnShardId.getIntervalStart(), Integer.toString(columnShardId.getShardNum()), Constants.LAST_ERROR));
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
        columnShardId.getInterval(),
        columnShardId.getIntervalStart(),
        Integer.toString(columnShardId.getShardNum()),
        Constants.LAST_ERROR,
        transaction,
        columnShardId.getColumnId().fullName()));
    try {
      Files.createDirectories(shardIdPath.getParent());
      long copied = Files.copy(inputStream, shardIdPath, REPLACE_EXISTING);
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

  @Override
  public ColumnMetadata getColumnMetadata(String tenant, String table, ColumnShardId columnShardId) {
    String currentPath = resolveCurrentPath(columnShardId.getShardId());
    if (currentPath == null)
      return null;
    Path shardIdPath = basePath.resolve(Paths.get(currentPath, columnShardId.getColumnId().fullName()));
    try {
      if (!Files.exists(shardIdPath)) {
        return null;
      } else {
        ColumnFileReader reader = new ColumnFileReader();
        reader.read(new DataInputStream(Files.newInputStream(shardIdPath, StandardOpenOption.READ)), null);
        return reader.getColumnMetadata();
      }
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  private String resolveCurrentPath(String tenant, String table) {
    DistXact status = getCurrentValues(tenant, table);
    if (status == null || status.getCurrent() == null)
      return null;
    return basePath.resolve(Paths.get(tenant, table, status.getCurrent())).toString();
  }

  private String resolveCurrentPath(ShardId shardId) {
    DistXact status = getCurrentValues(shardId);
    if (status == null || status.getCurrent() == null)
      return null;
    return basePath.resolve(Paths.get(shardId.shardIdPath(), status.getCurrent())).toString();
  }

  private DistXact getCurrentValues(String tenant, String table) {
    Path searchPath = basePath.resolve(DistXactUtil.buildCurrentMarker(Paths.get(tenant, table).toString()));
    if (!Files.exists(searchPath))
      return null;
    else {
      try (InputStream is = Files.newInputStream(searchPath)) {
        return DistXactUtil.readXactStatus(is);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  private DistXact getCurrentValues(ShardId shardId) {
    Path searchPath = basePath.resolve(DistXactUtil.buildCurrentMarker(Paths.get(shardId.shardIdPath()).toString()));
    if (!Files.exists(searchPath))
      return null;
    else {
      try (InputStream is = Files.newInputStream(searchPath)) {
        return DistXactUtil.readXactStatus(is);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  public void saveCurrentValues(String tenant, String table, String current, String previous) {
    Path searchPath = basePath.resolve(DistXactUtil.buildCurrentMarker(Paths.get(tenant, table).toString()));
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

  public void saveCurrentValues(ShardId shardId, String current, String previous) {
    Path searchPath = basePath.resolve(DistXactUtil.buildCurrentMarker(Paths.get(shardId.shardIdPath()).toString()));
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

  private void copyDirectory(Path src, Path dst) {
    try (Stream<Path> paths = Files.walk(src)) {
      AtomicReference<Path> current = new AtomicReference<>();
      paths.filter(file -> !src.equals(file)).forEach(
          file -> {
            try {
              if (file.endsWith("CURRENT")) {
                current.set(file);
              } else {
                Files.copy(file, dst.resolve(src.relativize(file)), REPLACE_EXISTING);
              }
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
      );
      if (current.get() != null) {
        Files.copy(current.get(), dst.resolve(src.relativize(current.get())), REPLACE_EXISTING);
      }
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private void deleteDirectory(Path directory) {
    try (Stream<Path> paths = Files.walk(directory)) {
      paths.forEach(
          source -> {
            try {
              Files.delete(source);
            } catch (IOException exception) {
              throw new RuntimeException(exception);
            }
          }
      );
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  @Override
  public void deleteTable(String tenant, String table) {
    try {
      Path toDelete = basePath.resolve(Paths.get(tenant, table));
      Files.walk(toDelete)
          .sorted(Comparator.reverseOrder())
          .map(Path::toFile)
          .forEach(File::delete);
    } catch (Exception e) {
      LOGGER.warn("Unable completely remove tenant {}", tenant, e);
    }
  }
}
